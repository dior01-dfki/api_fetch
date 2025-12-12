from clearml import Task, Dataset
import argparse
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def main(building_id:int):
    # Load all required tokens from environment
    git_token = os.environ['GIT_OAUTH_TOKEN']
    building_token = os.environ['buildings_token']
    hca_token = os.environ['hca_token']
    room_token = os.environ['rooms_token']
    units_token = os.environ['units_token']
    hca_details_token = os.environ['hca_details_token']
    room_details_token = os.environ['room_details_token']
    docker_env_args = (
        f"-e CLEARML_AGENT_GIT_USER=oauth2 "
        f"-e CLEARML_AGENT_GIT_PASS={git_token} "
        f"-e buildings_token={building_token} "
        f"-e hca_token={hca_token} "
        f"-e rooms_token={room_token} "
        f"-e units_token={units_token} "
        f"-e hca_details_token={hca_details_token} "
        f"-e room_details_token={room_details_token}"
        #f"--env-file {env_file_path} "
    )

    #print(docker_env_args)
    task = Task.create(
        project_name="ForeSightNEXT/BaltBest",
        task_name=f"Fetch Building Data Remotely-building {building_id}",
        script = "./cml_dataset.py",
        docker="nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04",
        docker_args=docker_env_args,
        argparse_args=[("building_id", building_id)],

    )


    Task.enqueue(task=task, queue_name="default")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="args to handle different building ids")
    parser.add_argument(
        '--building_id',
        type=int,
        default=57,
        help="specify the building id"
    )
    args = parser.parse_args()
    print(args.building_id)
    main(args.building_id)