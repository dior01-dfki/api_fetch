from clearml import Task, Dataset
import argparse

def main(building_id:int):
    task = Task.create(
        project_name="ForeSightNEXT/BaltBest",
        task_name="Fetch Building Data Remotely",
        script = "./cml_dataset.py",
        argparse_args=[("bulding_id", building_id)],
        docker="dior00002/heating-forecast2:v1"
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