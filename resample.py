import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from typing import List
from meteostat import Point, Daily, Hourly
from clearml import Task, Dataset
import os
from joblib import Parallel, delayed

def calculate_hi_res_roomwise(df_htd, df_hca):


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
    print(f"df_htd head before merge: {df_htd.head()}")
    print(f"df_hca head before merge: {df_hca.head()}")
    df = pd.merge(df_htd, df_hca, on='heat_cost_allocator_id', how='left')

    
    # 4. Calculate `q_hkv_dt` using the condition and formula
    df['q_hkv_dt'] = np.where(
        (df['temperature_2'] - df['temperature_1']) > 3,
        df['qs'] * np.power(df['kcw'] * df['kcl'] * (df['temperature_2'] - df['temperature_1']) / 60.0, 1.3) * df['dt'] / 1000.0,
        0.0
    )

    # Final DataFrame with columns: 'heat_cost_allocator_id', 'ts', 'q_hkv_dt'
    result = (
        df
        .groupby(['room_id','ts'])
        .agg(
            {'temperature_1':'max',
             "temperature_2":'max',
             'q_hkv_dt':'sum',
            'outside_temp':'mean'
             }
        ).sort_index()
    ).reset_index()
    #result = df[['heat_cost_allocator_id', 'ts','temperature_1','temperature_2', 'q_hkv_dt','room_id','outside_temp']]
    return result

def geocoding(cities:List[str]):
    geo_data = {}

    geolocator = Nominatim(user_agent="agent")
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

def room_resample(df):
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    
    #
    #city = building_metadata[building_metadata['building_id']==building_id]['city'].values[0]
    #latitude,longitude = geo_data[city]['latitude'], geo_data[city]['longitude']
    #meteo_data = fetch_meteodata(latitude, longitude, start, end)
    #print(f"meteo_data head: {meteo_data.head()}")
    #print(f"room data head: {df.head()}")
    hourly_room = (
        df
        .set_index('ts')
        .groupby('room_id')[['temperature']]
        .resample('h')
        .mean()
    )
    #hourly_room = hourly_room.join(meteo_data.set_index('ts'), how='left')
    return hourly_room

def hca_resample(df, building_id:int,building_metadata:pd.DataFrame):

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
    print(f"meteo_data head: {meteo_data.head()}")
    
    hourly_alloc = (
        df
        .set_index('ts')
        .groupby('heat_cost_allocator_id')[['temperature_1','temperature_2']]
        .resample('h')
        .mean()
    )
    hourly_alloc = hourly_alloc.join(meteo_data.set_index('ts'), how='left')
    return hourly_alloc

def clean_df(df):
    return df.loc[:,~df.columns.str.contains('^Unnamed')]

def main(building_id:int):

    #building_id = 13
    local_path = get_local_copy(building_id)
    building_metadata = pd.read_csv(f"{local_path}/building_metadata.csv")

    # Room data resampling and merging with meteodata
    try:
        df_room = pd.read_csv(f"{local_path}/building-{building_id}/room_temp_ts.csv", compression='gzip')
    except Exception:
        df_room = pd.read_csv(f"{local_path}/building-{building_id}/room_temp_ts.csv")

    df_room_resampled = room_resample(df_room)
    df_room_resampled.reset_index(inplace=True)
    hca_metadata = pd.read_csv(f"{local_path}/hca_metadata.csv")
    
    print(df_room_resampled.head())

    # HCA data resampling and hi-res unit calculation
    try:
        df_hca = pd.read_csv(f"{local_path}/building-{building_id}/allocator_ts.csv", compression='gzip')
    except Exception:
        df_hca = pd.read_csv(f"{local_path}/building-{building_id}/allocator_ts.csv")
        
    df_hca_resampled = hca_resample(df_hca,building_id=building_id, building_metadata=building_metadata)
    
    #df_units = pd.read_csv(f"{local_path}/building-{building_id}/units_ts.csv", compression='gzip',index_col=0)
    df_units_resampled = calculate_hi_res_roomwise(df_hca_resampled.reset_index(), hca_metadata)


    df_room_resampled.set_index(['room_id','ts'],inplace=True)
    
    combined = df_units_resampled.set_index(['room_id','ts']).join(df_room_resampled, how='outer')
    combined.sort_index(level=['room_id','ts'], inplace=True)

    print(combined.head())

    #combined = combined.join(df_hca_resampled, how='outer')
    combined.rename(columns={'q_hkv_dt':'hca_units',
                                'temperature':'inside_temp',
                                'temperature_2':'heater_side_hca_temp',
                                'temperature_1':'room_side_hca_temp',
                                'outside_temp':'outside_temp'
                                }, inplace=True)
    print(f"final combined.head():\n{combined.head()}")
    print(f"rooms present in building {building_id}: {combined.index.get_level_values('room_id').nunique()}")
    combined['building_id'] = building_id
    combined.reset_index(inplace=True)
    print(f"rooms present in building {building_id}: {combined['room_id'].nunique()}")
    combined = clean_df(combined)
    return combined

def safe_main(building_id:int):
    try:
        result = main(building_id)
        print(f"Completed processing for building {building_id}")
        return result
    except Exception as e:
        print(f"Error processing building {building_id}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def get_local_copy(building_id:int):
    dataset = Dataset.get(dataset_project='ForeSightNEXT/BaltBest',dataset_name=f"Building-{building_id}", dataset_version='0.0.1')
    local_path = dataset.get_local_copy()

    return local_path

def resample_remote():
    task = Task.init(project_name='ForeSightNEXT/BaltBest', task_name='Resample Test Remote Execution')
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")
    #building_ids = [58, 26, 57, 52, 17, 2, 45, 16, 47, 50, 28, 13, 46, 39, 14, 53, 18, 73, 7, 66, 38, 74, 4, 20, 23, 21, 10, 24, 48, 5, 31]
    building_ids = [4, 7, 10, 13, 14, 16, 17, 18, 20, 21, 23, 24, 26, 28, 38, 39, 45, 46, 47, 50, 52, 53, 57, 58, 66, 73, 74]
    #res = main(20)
    absolute_path = f"/tmp/resampled"
    os.makedirs(absolute_path, exist_ok=True)

    #res.to_csv("resampled_building_20.csv",index=False)
    #print(res.room_id.unique())
    results = Parallel(n_jobs=4)(delayed(safe_main)(building_id) for building_id in building_ids)
    final_df = pd.concat(results, ignore_index=True)
    final_df.to_csv(f'{absolute_path}/resampled_data.csv', index=False)
    Task.current_task().upload_artifact(name="resampled_data", artifact_object=final_df)
    new_dataset = Dataset.create(
        dataset_project='ForeSightNEXT/BaltBest/resampled',
        dataset_name='ResampledData',
        dataset_version='0.0.1'
    )
    new_dataset.add_files(f'{absolute_path}/resampled_data.csv')
    new_dataset.upload()
    new_dataset.finalize()
def fetch_units(path,building_id:int):
    try:
        try:
            df_units = pd.read_csv(f"{path}/building-{building_id}/units_ts.csv", compression='gzip', index_col=0)
        except Exception:
            df_units = pd.read_csv(f"{path}/building-{building_id}/units_ts.csv", index_col=0)
    except Exception as e:
        print(f"Error reading units data for building {building_id}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    hca_metadata = pd.read_csv(f"{path}/hca_metadata.csv")
    hca_metadata = hca_metadata[['heat_cost_allocator_id','room_id']]
    rooms_metadata = pd.read_csv(f"{path}/rooms_metadata.csv")
    rooms_metadata = rooms_metadata[['room_id','building_id']]
    metadata = hca_metadata.merge(rooms_metadata, on='room_id',how='inner')
    df = df_units.merge(metadata, on='heat_cost_allocator_id',how='inner')
    return df
    #return df_units

def fetch_units_remote():
    task = Task.init(
        project_name='ForeSightNEXT/BaltBest',
        task_name='Fetch Units Remote Execution'
    )
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")

    dataset = Dataset.get(
        dataset_project='ForeSightNEXT/BaltBest',
        dataset_name="BaltBest",
        dataset_version='0.0.1'
    )
    local_path = dataset.get_local_copy()

    building_ids = [4, 7, 10, 13, 14, 16, 17, 18, 20, 21, 23, 24, 26, 28, 38, 39, 45, 46, 47, 50, 52, 53, 57, 58, 66, 73, 74]

    res = Parallel(n_jobs=4)(
        delayed(fetch_units)(
            local_path,
            bid
        )
        for bid in building_ids
    )

    final_df = pd.concat(res, ignore_index=True)
    Task.current_task().upload_artifact(
        name="all_units_data",
        artifact_object=final_df
    )
# 
if __name__ == "__main__":
    #remote_test()
    # building_ids = [4, 7, 10, 13, 14, 16, 17, 18, 20, 21, 23, 24, 26, 28, 38, 39, 45, 46, 47, 50, 52, 53, 57, 58, 66, 73, 74]
    # print(len(building_ids))
    # fetch_units_remote()
    resample_remote()
    

    