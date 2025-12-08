from clearml import Task, Dataset
import argparse
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def main(building_id:int):
    # Load all required tokens from environment
    git_token = os.environ['GIT_OAUTH_TOKEN']
    buildings_token = os.getenv("BUILDINGS")
    hca_token = os.getenv("HEAT_COST_ALLOCATORS")
    rooms_token = os.getenv("ROOMS")
    units_token = os.getenv("UNITS")
    hca_details_token = os.getenv("HEAT_COST_ALLOCATOR_DETAILS")
    room_details_token = os.getenv("ROOM_DETAILS")
    
    env_file_path = os.path.abspath(".env")
    
    docker_env_args = (
        f"-e CLEARML_AGENT_GIT_USER=oauth2 "
        f"-e CLEARML_AGENT_GIT_PASS={git_token} "
        f"--env buildings_token={buildings_token} "
        f"--env hca_token={hca_token} "
        f"--env rooms_token={rooms_token} "
        f"--env units_token={units_token} "
        f"--env hca_details_token={hca_details_token} "
        f"--env room_details_token={room_details_token} "
        #f"-v {env_file_path}:/app/.env "
        #f"--env-file {env_file_path} "
    )
    #print(docker_env_args)
    task = Task.create(
        project_name="ForeSightNEXT/BaltBest",
        task_name="Fetch Building Data Remotely",
        script = "./cml_dataset.py",
        argparse_args=[("--building_id", building_id)],
        docker="dior00002/heating-forecast2:v1",
        docker_args=docker_env_args
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
    main(args.building_id)