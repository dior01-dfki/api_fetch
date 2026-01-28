from clearml import Task, Dataset
import os
import pandas as pd


def main():
    test_building_ids = [58,41,16]
    task = Task.init(project_name='ForeSightNEXT/BaltBest', task_name='Split Train Test Data')
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")

    dataset = Dataset.get(dataset_name='ResampledData', dataset_project='ForeSightNEXT/BaltBest/resampled', dataset_version="0.0.1")
    local_path = dataset.get_local_copy()
    resampled = pd.read_csv(f"{local_path}/resampled_data.csv",index_col = 0)
    resampled['ts'] = pd.to_datetime(resampled['ts'])
    test_df = resampled[resampled['building_id'].isin(test_building_ids)]
    train_df = resampled[~resampled['building_id'].isin(test_building_ids)]

    os.makedirs('temp', exist_ok=True)
    train_out_path = 'temp/train_data.csv'
    train_df.to_csv(train_out_path,index=False)
    test_out_path = 'temp/test_data.csv'
    test_df.to_csv(test_out_path,index=False)
    dataset_train = Dataset.create(dataset_name='TrainData', dataset_project='ForeSightNEXT/BaltBest/resampled', parent_datasets=[dataset.id],dataset_version="0.0.1")
    dataset_train.set_description("Training data excluding test buildings: [58,41,16]")
    print(f"Uploaded training data with {len(train_df)} records.")
    dataset_train.add_files(train_out_path)
    dataset_train.upload()
    dataset_train.finalize()
    print(f"Uploaded test data with {len(test_df)} records.")
    dataset_test = Dataset.create(dataset_name='TestData', dataset_project='ForeSightNEXT/BaltBest/resampled', parent_datasets=[dataset.id],dataset_version="0.0.1")
    dataset_test.set_description("Test data for buildings: [58,41,16]")
    dataset_test.add_files(test_out_path)
    dataset_test.upload()
    dataset_test.finalize()
    print("Datasets created and uploaded successfully.")
if __name__ == "__main__":
    main()