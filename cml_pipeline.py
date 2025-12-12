from clearml.automation.controller import PipelineController
from clearml import Task
import pandas as pd


PIPELINE_PROJECT_NAME = "ForeSightNEXT/BaltBest"
PIPELINE_TASK_NAME = "Building Data Fetch Pipeline"


BASE_TASK_PROJECT = "ForeSightNEXT/BaltBest"
BASE_TASK_NAME = "Fetch Building Data Remotely" 

def main(building_ids):
    """
    Initializes and executes the ClearML Pipeline.
    """

    pipe = PipelineController(
        project=PIPELINE_PROJECT_NAME,
        name=PIPELINE_TASK_NAME,
        version="1.0.0",
        add_pipeline_args=False,
    )


    try:
        base_task = Task.get(
            project_name=BASE_TASK_PROJECT,
            task_name=f"{BASE_TASK_NAME}-building 2" 
        )
        base_task_id = base_task.id
    except Exception as e:
        print(f"Could not find a base task for the step: {e}")
        print("Ensure you have run entry.py at least once to create a template task.")
        return


    step_list = []
    for building_id in building_ids:

        task_name = f"{BASE_TASK_NAME}-building {building_id}"


        pipe.add_step(
            name=f"Building_{building_id}_Fetch",
            base_task_id=base_task_id,
            
            parameter_override={
                'Task/name': task_name,
                'Args/building_id': building_id
            },
        )
        step_list.append(f"Building_{building_id}_Fetch")


    print(f"Executing pipeline on buildings: {building_ids}")
    #pipe.start_locally(queue="default") 
    pipe.start(queue="default", steps=step_list)
    print("Pipeline execution initiated.")


if __name__ == "__main__":
    building_ids = pd.read_csv("metadata/rooms_metadata.csv")['building_id'].unique().tolist()
    building_ids = [i for i in building_ids if i not in [2,38]]
    main(building_ids)