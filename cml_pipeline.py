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
        version="1.0.1",
    )


    try:
        base_task = Task.get_task(
            project_name=BASE_TASK_PROJECT,
            task_name=f"{BASE_TASK_NAME}-building 2" 
        )
        base_task_id = base_task.id
    except Exception as e:
        print(f"Could not find a base task for the step: {e}")
        print("Ensure you have run entry.py at least once to create a template task.")
        return


    
    previous_step = None

    for building_id in building_ids:

        task_name = f"{BASE_TASK_NAME}-building {building_id}"
        step_name = f"Building_{building_id}_Fetch"

        pipe.add_step(
            name=step_name,
            base_task_id=base_task_id,
            parameter_override={
                'Task/name': task_name,
                'Args/building_id': building_id
            },
            execution_queue="default",
            parents=[previous_step] if previous_step else None,
            continue_behaviour={
                "continue_on_fail": True
    }
        )

        previous_step = step_name
        


    print(f"Executing pipeline on buildings: {building_ids}")
    #pipe.start_locally(queue="default") 
    pipe.start(queue="default")
    print("Pipeline execution initiated.")


if __name__ == "__main__":
    building_ids = pd.read_csv("metadata/rooms_metadata.csv")['building_id'].unique().tolist()
    building_ids = [i for i in building_ids if i not in [2,7,13,14,16,17,20,28,38,58,66,73,74]]
    main(building_ids)
    # [4, 10, 18, 21, 23, 24, 26, 39, 45, 46, 47, 50, 52, 53, 57]