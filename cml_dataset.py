from clearml import Dataset, Task
import requests
from dotenv import load_dotenv, find_dotenv
import os 
import pandas as pd
import argparse

# Initialize ClearML Task
# task = Task.init(
#     project_name="ForeSightNEXT/BaltBest",
#     task_name="Fetch Building Data"
# )

# Load environment variables (works locally with .env file)
load_dotenv(find_dotenv())
buildings_token = os.getenv("buildings_token")
hca_token = os.getenv("hca_token")
rooms_token = os.getenv("rooms_token")
units_token = os.getenv("units_token")
hca_details_token = os.getenv("hca_details_token")
room_details_token = os.getenv("room_details_token")

hca_metadata = pd.read_csv("metadata/hca_metadata.csv")
room_meta_data = pd.read_csv("metadata/rooms_metadata.csv")

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
    dataset.add_files('metadata/')
    dataset.upload()
    dataset.finalize()

def create_building_dataset(building_id):
    parent_dataset = Dataset.get(
        dataset_project=dataset_project,
        dataset_name=dataset_name,
        dataset_version="0.0.1",
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
    dataset.finalize()

def fetch_room_temps(room_id: int, room_details_token: str):
    all_data = []
    page = 1

    while True:
        print(f"Fetching room {room_id}, page {page}")

        resp = requests.get(
            f"{baseurl}/{room_id}/temperatures",
            headers={'Content-Type': 'application/json',
                     "Authorization": room_details_token},
            params={'per_page': 1000, 'page': page}
        )

        if not (200 <= resp.status_code < 300):
            print(f"Error fetching data for room {room_id}: {resp.status_code} - {resp.text}")
            break

        resp_json = resp.json()
        data = resp_json.get("data", [])
        num_pages = resp_json.get("num_pages", 1)
        curr_page = resp_json.get("page", page)

        all_data.extend(data)
        print(data)
        print(curr_page, num_pages)
        # Stop when server says “this is the last page”
        if curr_page >= num_pages:
            break

        if not data:  # defensive break
            break

        page += 1

    return all_data

 

def fetch_building_rooms(building_id:int, room_details_token:str):
    
    
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
    
    building__room_df = pd.concat(room_df_list, ignore_index=True)
    building__room_df.to_csv(f"room_temp_ts.csv", index=False)

    building__hca_df =  pd.concat(hca_df_list, ignore_index=True)
    building__hca_df.to_csv(f"allocator_ts.csv", index=False)

    units__hca_df = pd.concat(units_df_list, ignore_index=True)
    units__hca_df.to_csv(f"units_ts.csv",index=False)

def fetch_room_hcas(room_id:int, hca_details_token:str):
    
    
    room_hcas = hca_metadata[(hca_metadata['room_id'] == room_id)]
    unique_hcas = room_hcas['heat_cost_allocator_id'].unique()
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
        room_hca_df = pd.DataFrame()
    try:
        units_df = pd.concat(df_list_units, ignore_index=True)
    except ValueError:
        units_df = pd.DataFrame()
    return room_hca_df, units_df

def fetch_hca_temps(hca_id:int, hca_details_token:str):
    
    all_data = []
    page = 1
    while True:
        print(f"Fetching HCA {hca_id}, page {page}")
        resp = requests.get(
            f"{baseurl}/{hca_id}/temperatures",
            headers={'Content-Type': 'application/json',"Authorization": hca_details_token},
            params={'per_page': 1000, "page": page}
        )
        page += 1
        if 200 <= resp.status_code < 300:
            
            resp_data = resp.json()
            
            all_data.extend(resp_data)
            if resp_data == []:
                break
        else:
            print(f"Error fetching data for HCA {hca_id}: {resp.status_code} - {resp.text}")
            break
    return all_data

def fetch_hca_units(hca_id:int, hca_details_token:str):
    resp = requests.get(
        f"{baseurl}/{hca_id}/units",
        headers={'Content-Type': 'application/json',"Authorization": hca_details_token},
    )
    if 200 <= resp.status_code < 300:
        return resp.json()
    else:
        print(f"Error fetch in unit data for HCA {hca_id}: {resp.status_code} - {resp.text}")
        return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--building_id", type=int, default=57, help="Building ID to fetch")
    # #parser.add_argument("--remote", action="store_true", help="Execute remotely on ClearML agent")
    
    args = parser.parse_args()
    
    # # If remote flag is set, configure and execute remotely
    # #if args.remote:

    
    task.execute_remotely(
    queue_name="default"
    )


    

    # # print(f"Fetching building: {args.building_id}")
    fetch_building_rooms(args.building_id, room_details_token)
    create_building_dataset(args.building_id)

    