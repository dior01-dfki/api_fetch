import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from typing import List
from meteostat import Point, Daily, Hourly
from clearml import Task, Dataset
import os

def calculate_hi_res(df_htd, df_hca):


    # df_htd = pd.read_csv(data_root.joinpath('heat_cost_allocator_temp_data.csv'))

    # df_hca = pd.read_csv(data_root.joinpath('heat_cost_allocator.csv'))
    df_htd["ts"] = pd.to_datetime(df_htd["ts"],utc=True,errors='coerce')
    #df_htd['ts'] = df_htd['ts'].dt.tz_localize(None)

    # 1. Sort the data to simulate SQL's `PARTITION BY` and `ORDER BY`
    df_htd = df_htd.sort_values(by=['heat_cost_allocator_id', 'ts'])

    # 2. Calculate the time difference (`dt`) between current and previous timestamps (in hours)
    df_htd['dt'] = (pd.to_datetime(df_htd['ts']) - pd.to_datetime(df_htd['ts']).shift(1)) / np.timedelta64(1, 'h')

    # Set `dt` to NaN where the `heat_cost_allocator_id` changes (similar to SQL `PARTITION BY`)
    df_htd['dt'] = np.where(df_htd['heat_cost_allocator_id'] == df_htd['heat_cost_allocator_id'].shift(1), df_htd['dt'], np.nan)

    # 3. Merge with `heat_cost_allocator` to get `qs`, `kcw`, and `kcl` columns
    df = pd.merge(df_htd, df_hca, on='heat_cost_allocator_id', how='left')

    # 4. Calculate `q_hkv_dt` using the condition and formula
    df['q_hkv_dt'] = np.where(
        (df['temperature_2'] - df['temperature_1']) > 3,
        df['qs'] * np.power(df['kcw'] * df['kcl'] * (df['temperature_2'] - df['temperature_1']) / 60.0, 1.3) * df['dt'] / 1000.0,
        0.0
    )

    # Final DataFrame with columns: 'heat_cost_allocator_id', 'ts', 'q_hkv_dt'
    result = df[['heat_cost_allocator_id', 'ts', 'q_hkv_dt']]
    return result

def geocoding(cities:List[str]):
    geo_data = {}

    geolocator = Nominatim(user_agent="your_app_name")
    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=1.1,
        max_retries=2,
        error_wait_seconds=5
    )

    for city in cities:
        location = geocode(f"{city}, Germany", timeout=10)

        if location is None:
            continue

        geo_data[city] = {
            "latitude": location.latitude,
            "longitude": location.longitude
        }

    return geo_data

def fetch_meteodata(latitude, longitude,start: pd.Timestamp, end: pd.Timestamp):
    location = Point(latitude, longitude)
    meteo_data = Hourly(location, start=start, end=end).fetch()
    meteo_data = meteo_data.reset_index()
    meteo_data = meteo_data[["time","temp"]]
    meteo_data = meteo_data.rename(columns={"time":"ts","temp":"outside_temp"})
    meteo_data["ts"] = pd.to_datetime(meteo_data["ts"], utc=True)

    return meteo_data

def alloc_resample(df):
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    hourly_alloc = (
        df
        .set_index('ts')
        .groupby('heat_cost_allocator_id')[['temperature_1', 'temperature_2']]
        .resample('h')
        .mean()
    )
    return hourly_alloc

def room_resample(df,building_id, building_metadata):
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    start = (
        df["ts"]
        .min()
        .tz_convert("UTC")
        .tz_localize(None)
        .floor("D")
    )

    end = (
        df["ts"]
        .max()
        .tz_convert("UTC")
        .tz_localize(None)
        .ceil("D")
    )
    geo_data = geocoding(building_metadata['city'].unique().tolist())
    city = building_metadata[building_metadata['building_id']==building_id]['city'].values[0]
    latitude,longitude = geo_data[city]['latitude'], geo_data[city]['longitude']
    meteo_data = fetch_meteodata(latitude, longitude, start, end)
    hourly_room = (
        df
        .set_index('ts')
        .groupby('room_id')[['temperature']]
        .resample('h')
        .mean()
    )
    hourly_room = hourly_room.join(meteo_data.set_index('ts'), how='left')
    return hourly_room

def hca_resample(df):
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    hourly_alloc = (
        df
        .set_index('ts')
        .groupby('heat_cost_allocator_id')[['temperature_1','temperature_2']]
        .resample('h')
        .mean()
    )
    return hourly_alloc

def units_resample(df, hourly_alloc):
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.floor("D")
    hi_res_units = calculate_hi_res(hourly_alloc.reset_index(), df)
    return hi_res_units

def main(local_path:str):
    building_id = 13
    building_metadata = pd.read_csv(f"{local_path}/building_metadata.csv")

    # Room data resampling and merging with meteodata
    df_room = pd.read_csv(f"{local_path}/Building-{building_id}/room_temp_ts.csv",compression='gzip',index_col=0)
    df_room_resampled = room_resample(df_room,building_id, building_metadata)
    df_room_resampled.reset_index(inplace=True)
    hca_metadata = pd.read_csv(f"{local_path}/hca_metadata.csv")
    df_room_resampled = df_room_resampled.merge(hca_metadata[['heat_cost_allocator_id','room_id']], on='room_id', how='left')
    print(df_room_resampled.head())

    # HCA data resampling and hi-res unit calculation
    df_hca = pd.read_csv(f"{local_path}/Building-{building_id}/allocator_ts.csv",compression='gzip')
    df_hca_resampled = hca_resample(df_hca)
    df_units = pd.read_csv(f"{local_path}/Building-{building_id}/units_ts.csv",compression='gzip',index_col=0)
    df_units_resampled = units_resample(df_units, df_hca_resampled)
    df_room_resampled.set_index(['heat_cost_allocator_id','ts'],inplace=True)
    combined = df_room_resampled.join(df_units_resampled.set_index(['heat_cost_allocator_id','ts']), how='left')
    combined.sort_index(level=['heat_cost_allocator_id','ts'], inplace=True)

    print(combined.head())

def get_local_copy(building_id:int):
    dataset = Dataset.get(dataset_project='ForeSightNEXT/BaltBest',dataset_name=f"Building-{building_id}", dataset_version='0.0.1')
    local_path = dataset.get_local_copy()
    print(f"Dataset local path: {local_path}")
    print(f"Contents: {os.listdir(local_path)}")
    return local_path

def remote_test():
    task = Task.init(project_name='ForeSightNEXT/BaltBest', task_name='Resample Test Remote Execution')
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")

    main(get_local_copy(50))

# 
if __name__ == "__main__":
    remote_test()
    

    