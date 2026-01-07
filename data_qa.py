import pandas as pd
import numpy as np
from clearml import Dataset, Task

def fix_yearly_reset(hca_units: pd.DataFrame) -> pd.DataFrame:
    hca_units = hca_units.sort_values(['heat_cost_allocator_id', 'ts']).copy()

    is_jan1 = (hca_units['ts'].dt.month == 1) & (hca_units['ts'].dt.day == 1)

    offset = (
        hca_units['units']
        .shift()
        .where(is_jan1, 0)
        .groupby(hca_units['heat_cost_allocator_id'])
        .cumsum()
    )

    hca_units['units'] = hca_units['units'] + offset
    return hca_units



def align_hca(resampled: pd.DataFrame, hca_units:pd.DataFrame) -> pd.DataFrame:
    
    
    hca_units = hca_units[hca_units['units'].notna()]
    resampled.ts = pd.to_datetime(resampled.ts)
    hca_units.ts = pd.to_datetime(hca_units.ts)

    if ((hca_units['units'] == 0).all() or hca_units.empty):
        return pd.DataFrame()
    
    hca_units = hca_units.sort_values(['room_id','ts'])

    hca_units = fix_yearly_reset(hca_units)
    hca_units = hca_units.resample('D', on='ts').agg({'units':'sum'}).reset_index()
    res = resampled.copy()
    res = res.resample('D', on='ts').agg({'hca_units':'sum'}).reset_index()
    hca_units['delta'] = hca_units['units'].diff()
    merged = pd.merge(res, hca_units, on=['ts'], how='inner')
    return merged

def calculate_mape_rmse(resampled:pd.DataFrame, hca_units:pd.DataFrame) -> tuple:
    merged = align_hca(resampled, hca_units)
    if merged.empty:
        return np.nan, np.nan
    merged = merged[merged.hca_units != 0]
    merged['ape'] = (merged.delta- merged.hca_units).abs() / merged.hca_units.abs()
    merged['squared_error'] = (merged.delta - merged.hca_units) ** 2
    return merged.ape.mean() * 100, merged.squared_error.mean() ** 0.5

def count_na(col:pd.Series):
    is_na = col.isna()
    groups = is_na.ne(is_na.shift()).cumsum()
    na_runs = is_na.groupby(groups).sum()
    na_runs = na_runs[na_runs > 0]
    out = {f'n_gaps_gte_{i}h':0 for i in range(1,13)}
    out['n_gaps_gte_>12h'] = 0
    for run in na_runs:
        if run > 12:
            out['n_gaps_gte_>12h'] += 1
        else:
            out[f'n_gaps_gte_{run}h'] += 1
    return out

def consecutive_vals(col:pd.Series):
    not_na = col.notna()
    groups = not_na.ne(not_na.shift()).cumsum()
    val_runs = not_na.groupby(groups).sum()
    val_runs = val_runs[val_runs > 0]
    bins = {
        '1d': 24,
        '2d': 2*24,
        '4d': 4*24,
        '7d': 7*24,
        '14d': 14*24
    }
    count_out = {k: 0 for k in bins}
    sum_out   = {k: 0 for k in bins}
    for run in val_runs:
        for k, threshold in reversed(bins.items()):
            if run >= threshold:
                count_out[k] += 1
                sum_out[k] += run
                #break is added to avoid double counting
                break
    #print(val_runs)
    out = {}
    for k in bins:
        out[f'n_consec_gte_{k}'] = count_out[k]
    for k in bins:    
        out[f'total_len_consec_gte_{k}'] = sum_out[k]
    return out

def df_qa(resampled: pd.DataFrame, hca_units:pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in resampled.columns if c not in ['room_id', 'ts', 'building_id']]
    resampled = resampled.sort_values(['room_id', 'ts'])
    all_rooms = []

    for room_id, group in resampled.groupby('room_id'):
        
        mape, rmse = calculate_mape_rmse(group[['hca_units','ts']], hca_units[hca_units.room_id==room_id])
        for col in cols:
            # compute gaps and consecutive-value summaries
            gap_summary = count_na(group[col])
            val_summary = consecutive_vals(group[col])

            #mape, rmse = calculate_mape_rmse()
            # combine into a single row, flattening keys
            combined = {}
            combined.update({f'{k}': v for k, v in gap_summary.items()})
            combined.update({f'{k}': v for k, v in val_summary.items()})
            combined.update({'n_rows': len(group[col])})
            #combined.update({'total_non_null': group[col].notna().sum()})
            combined.update({'n_nan_rows': group[col].isna().sum()})
            combined.update({'non_nan_ratio':group[col].notna().sum() / len(group[col])})
            combined['room_id'] = room_id
            combined['variable'] = col
            combined['upsampling_mape'] = mape
            combined['upsampling_rmse'] = rmse
            all_rooms.append(combined)

    # create final DataFrame and set MultiIndex
    result = pd.DataFrame(all_rooms)
    result = result.set_index(['room_id', 'variable'])
    return result


def main():
    task = Task.init(project_name='ForeSightNEXT/BaltBest', task_name='Data QA')
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")


    dataset = Dataset.get(dataset_name='ResampledData', dataset_project='ForeSightNEXT/BaltBest/resampled', dataset_version="0.0.1")
    local_path = dataset.get_local_copy()
    resampled = pd.read_csv(f"{local_path}/resampled_data.csv",index_col = 0)
    resampled['ts'] = pd.to_datetime(resampled['ts'])

    print(f"resampled.head():\n{resampled.head()}")

    unit_task = Task.get_task(task_name='Fetch Units Remote Execution', project_name='ForeSightNEXT/BaltBest',task_id='0d438a74ff5c4cbf99ecc8725437f1da')
    data_path = unit_task.artifacts['all_units_data'].get_local_copy()
    try:
        hca_units = pd.read_csv(data_path,index_col = 0)
    except Exception as e:
        hca_units = pd.read_csv(data_path, compression='gzip', index_col = 0)
    hca_units['ts'] = pd.to_datetime(hca_units['ts'])
    hca_units.groupby('room_id').resample('D', on='ts').agg({'units':'sum'}).reset_index()

    print(f"hca_units.head():\n{hca_units.head()}")
    result = df_qa(resampled, hca_units)
    Task.current_task().upload_artifact('data_qa_report', artifact_object=result)


    # meta_dataset = Dataset.get(dataset_name='BaltBestMetadata', dataset_project='ForeSightNEXT/BaltBest', dataset_version="0.0.1")
    # new_dataset = Dataset.create(
    #     dataset_name='BaltBestMetadata',
    #     dataset_project='ForeSightNEXT/BaltBest',
    #     dataset_version='0.0.2',
    #     parent_datasets=[meta_dataset.id],
    # )
    # new_dataset.set_description("This version of metadata includes data QA report done by DFKI. data_qa_report.csv is not part of EBZ")
    # new_dataset.add_files(path_or_file=result.to_csv(index=True), dataset_path='data_qa_report.csv')
    # new_dataset.upload()
    # new_dataset.finalize()

if __name__ == "__main__":
    main()