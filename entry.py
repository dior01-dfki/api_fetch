from clearml import Task, Dataset
import argparse
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def main(building_id:int):
    # Load all required tokens from environment
    git_token = os.environ['GIT_OAUTH_TOKEN']
    
    docker_env_args = (
        f"-e CLEARML_AGENT_GIT_USER=oauth2 "
        f"-e CLEARML_AGENT_GIT_PASS={git_token} "
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