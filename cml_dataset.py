from clearml import Dataset, Task
import requests
from dotenv import load_dotenv, find_dotenv
import os 
import pandas as pd
import argparse


load_dotenv(find_dotenv())
buildings_token = os.getenv("BUILDINGS")
hca_token = os.getenv("HEAT_COST_ALLOCATORS")
rooms_token = os.getenv("ROOMS")
units_token = os.getenv("UNITS")
hca_details_token = os.getenv("HEAT_COST_ALLOCATOR_DETAILS")
room_details_token = os.getenv("ROOM_DETAILS")


dataset_project: str = "ForeSightNEXT/BaltBest"
dataset_name: str = "BaltBestMetadata"
baseurl = "https://edc.e-b-z.de/public"


def create_meta_dataset():

    dataset = Dataset.create(
        dataset_project=dataset_project,
        dataset_name=dataset_name,
        dataset_tags=["test"],
        dataset_version="0.0.1",
    )

    dataset.add_files('metadata/')  # Adds files from a local folder to the dataset
    dataset.upload()      # Uploads the data to the ClearML server/storage


    dataset.finalize()    # Locks and versions the dataset

def create_building_dataset(building_id):
    parent_dataset = Dataset.get(
        dataset_project=dataset_project,
        dataset_name=dataset_name,
        dataset_version="0.0.1",
        #dataset_id="010471ad8cfa4ab2b323e9c8f4d2211b"
    )
    dataset = Dataset.create(
        dataset_project=dataset_project,
        dataset_name=f"Building-{building_id}",
        dataset_tags=["test"],
        dataset_version="0.0.1",
        parent_datasets=[parent_dataset],
    )
    dataset.add_files("room_temp_ts.csv")
    dataset.add_files("allocator_ts.csv")
    dataset.add_files("units_ts.csv")
    dataset.upload()
    dataset.finalize()    # Locks and versions the dataset

def fetch_room_temps(room_id:int, room_details_token:str):
    all_data = []
    page = 1
    while True:
        print(f"Fetching room {room_id}, page {page}")
        resp = requests.get(
            f"{baseurl}/{room_id}/temperatures",
            headers={'Content-Type': 'application/json',"Authorization": room_details_token},
            params={'per_page': 5000, 'page': page}
        )
        page += 1
        if 200 <= resp.status_code < 300:
            resp_data = resp.json()
            all_data.extend(resp_data['data'])
            if page > 5 or resp_data['data'] == []:
                break
            
        else:
            print(f"Error fetching data for room {room_id}: {resp.status_code} - {resp.text}")
            break
    return all_data
 

def fetch_building_rooms(building_id:int, room_details_token:str):
    room_meta_data = pd.read_csv("metadata/rooms_metadata.csv")
    #unique_buildings = room_meta_data['building_id'].unique()
    building_rooms = room_meta_data[room_meta_data['building_id'] == building_id]
    unique_rooms = building_rooms['room_id'].unique()
    room_df_list = []
    hca_df_list = []
    units_df_list = []
    for room in unique_rooms:
        room_data = fetch_room_temps(room, room_details_token)
        room_df = pd.json_normalize(room_data)
        room_df_list.append(room_df)

        hca_data, units_data = fetch_room_hcas(room, hca_details_token)
        hca_df_list.append(hca_data)
        units_df_list.append(units_data)
        #convert_to_csv(data, f"building_{building_id}_room_{room}_temps.csv")
    building__room_df = pd.concat(room_df_list, ignore_index=True)
    building__room_df.to_csv(f"room_temp_ts.csv", index=False)

    building__hca_df =  pd.concat(hca_df_list, ignore_index=True)
    building__hca_df.to_csv(f"allocator_ts.csv", index=False)

    units__hca_df = pd.concat(units_df_list, ignore_index=True)
    units__hca_df.to_csv(f"units_ts.csv",index=False)
    #return building__room_df

def fetch_room_hcas(room_id:int, hca_details_token:str):
    hca_metadata = pd.read_csv("metadata/hca_metadata.csv")
    room_hcas = hca_metadata[(hca_metadata['room_id'] == room_id)]
    unique_hcas = room_hcas['heat_cost_allocator_id'].unique()
    #print(unique_hcas)
    df_list = []
    df_list_units = []
    for hca in unique_hcas:
        data = fetch_hca_temps(hca, hca_details_token)
        df = pd.json_normalize(data)
        df_list.append(df)

        data_units = fetch_hca_units(hca,hca_details_token)
        df_units = pd.json_normalize(data_units)
        df_list_units.append(df_units)
        print(f"Room ID: {room_id}, HCA ID: {hca}, len df_list: {len(df_list)}")
    try:
        room_hca_df = pd.concat(df_list, ignore_index=True)
    except ValueError:
        room_hca_df = pd.DataFrame()  # Return an empty DataFrame if df_list is empty
    try:
        units_df = pd.concat(df_list_units, ignore_index=True)
    except ValueError:
        units_df = pd.DataFrame()
    #print(room_hca_df)
    #room_hca_df.to_csv(f"hca_temp_test.csv", index=False)
    return room_hca_df, units_df

def fetch_hca_temps(hca_id:int, hca_details_token:str):
    all_data = []
    page = 1
    while True:
        print(f"Fetching HCA {hca_id}, page {page}")
        resp = requests.get(
            f"{baseurl}/{hca_id}/temperatures",
            headers={'Content-Type': 'application/json',"Authorization": hca_details_token},
            params={'per_page': 5000, "page": page}
        )
        page += 1
        if 200 <= resp.status_code < 300:
            resp_data = resp.json()
            #print(resp_data)
            all_data.extend(resp_data)
            if resp_data == [] or page > 5:
                print(f"Fetched data for HCA {hca_id}: {resp_data}")
                break
            
        else:
            print(f"Error fetching data for HCA {hca_id}: {resp.status_code} - {resp.text}")
            break
    return all_data

def fetch_hca_units(hca_id:int, hca_details_token:str):
    all_data = []
    page = 1 
    while True:
        resp = requests.get(
            f"{baseurl}/{hca_id}/units",
            headers={'Content-Type': 'application/json',"Authorization": hca_details_token},
        )
        page += 1
        if 200 <= resp.status_code < 300:
            resp_data = resp.json()
            all_data.extend(resp_data)
            if resp_data == [] or page > 5:
                print(f"Fetched units for HCA {hca_id}: {resp_data}")
                break 
        else:
            print(f"Error fetch in unit data for HCA {hca_id}: {resp.status_code} - {resp.text}")
            break 
    return all_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--building_id", type=int, default=57, help="Building ID to fetch")


    args = parser.parse_args()
    print(f"Fetching building: {args.building_id}")
    fetch_building_rooms(args.building_id, room_details_token)
    #create_building_dataset(args.building_id)   

    # create_meta_dataset()
    #create_building_dataset()
    #data = fetch_room_temps(1000, room_details_token)
    #print(data)
    #convert_to_csv(data, "rooooom_test.csv")

