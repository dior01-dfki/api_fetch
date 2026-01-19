import pandas as pd
import numpy as np
from clearml import Dataset, Task
import os

def fix_reset(hca_units: pd.DataFrame) -> pd.DataFrame:
    hca_units = hca_units.sort_values(
        ['heat_cost_allocator_id', 'ts']
    ).copy()

    def _fix(group: pd.DataFrame) -> pd.DataFrame:
        prev_units = group['units'].shift()
        # detect reset when units drop to 0 after being positive
        is_reset = (group['units'] == 0) & (prev_units > 0)

        offset = (
            prev_units
            .where(is_reset, 0)
            .cumsum()
        )

        group['units'] = group['units'] + offset
        return group

    return (
        hca_units
        .groupby('heat_cost_allocator_id', group_keys=False)
        .apply(_fix)
    )



def align_hca(resampled: pd.DataFrame, hca_units:pd.DataFrame) -> pd.DataFrame:
    
    
    hca_units = hca_units[hca_units['units'].notna()]
    resampled.ts = pd.to_datetime(resampled.ts)
    hca_units.ts = pd.to_datetime(hca_units.ts)

    if ((hca_units['units'] == 0).all() or hca_units.empty):
        return pd.DataFrame()
    
    hca_units = hca_units.sort_values(['room_id','ts'])

    hca_units = fix_reset(hca_units)
    hca_units = hca_units.resample('D', on='ts').agg({'units':'sum'}).reset_index()
    res = resampled.copy()
    res = res.resample('D', on='ts').agg({'hca_units':'sum'}).reset_index()
    hca_units['delta'] = hca_units['units'].diff()
    #Need to think about this part of calculating hca_units delta, this part of the code fills negative deltas with 0
    #hca_units['delta'] = hca_units[hca_units['delta']].apply(lambda x: 0 if x < 0 else x)
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
                #break
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
            combined.update({'total_non_null': group[col].notna().sum()})
            combined.update({'n_nan_rows': group[col].isna().sum()})
            combined.update({'non_nan_ratio':group[col].notna().sum() / len(group[col])})
            combined.update({'non_zero_rows': (group[col] != 0).sum()})
            combined.update({'non_zero_ratio': (group[col] != 0).sum() / len(group[col])})
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


    meta_dataset = Dataset.get(dataset_name='BaltBestMetadata', dataset_project='ForeSightNEXT/BaltBest', dataset_version="0.0.1")
    new_dataset = Dataset.create(
        dataset_name='BaltBestMetadata',
        dataset_project='ForeSightNEXT/BaltBest',
        dataset_version='0.0.2',
        parent_datasets=[meta_dataset.id],
    )
    result.to_csv('data_qa_report.csv', index=True)
    new_dataset.set_description("This version of metadata includes data QA report done by DFKI. data_qa_report.csv is not part of EBZ")
    new_dataset.add_files(path='data_qa_report.csv')
    new_dataset.upload()
    new_dataset.finalize()

# def test_dataset():
#     dataset = Dataset.create(
#         dataset_name='AcceptableRooms',
#         dataset_project='ForeSightNEXT/BaltBest/resampled',
#         dataset_version='0.0.1',
#     )
#     dataset.add_files(path='acceptable_rooms.csv')
#     dataset.upload()
#     dataset.finalize()



def count_consec_vals(df,col,threshold=14*24):
    not_nan = df[col].notna()
    grp = not_nan.ne(not_nan.shift()).cumsum()
    run_len = not_nan.groupby(grp).transform('sum')
    if run_len.max() < threshold:
        return 0
    else:
        return 1

def non_zero_vals(df,col,threshold=14*24):
    not_zero = df[col] != 0
    grp = not_zero.ne(not_zero.shift()).cumsum()
    run_len = not_zero.groupby(grp).transform('sum')
    if run_len.max() < threshold:
        return 0
    else:
        return 1
def count_overlap_consec_vals(df,cols,target_col,threshold=14*24):
    valid = (
        df[list(cols)].notna().all(axis=1) &
        (df[target_col] != 0)
    )
    grp = valid.ne(valid.shift()).cumsum()
    run_len = valid.groupby(grp).transform('sum')
    if run_len.max() < threshold:
        return 0
    else:
        return 1 
def seasons_qa(resampled_df:pd.DataFrame, rooms_metadata:pd.DataFrame) -> pd.DataFrame:
    resampled_df['ts'] = pd.to_datetime(resampled_df['ts'])
    resampled_df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
    rooms_metadata = rooms_metadata[['unit_id','room_id']]
    resampled_df['year'] = resampled_df['ts'].dt.year
    resampled_df = resampled_df.merge(rooms_metadata, on='room_id', how='left')
    rows = []
    for (room_id,unit_id,building_id,year), group in resampled_df.groupby(['room_id','unit_id','building_id','year']):
        inside_temp_season = count_consec_vals(group,'inside_temp')
        outside_temp_season = count_consec_vals(group,'outside_temp')
        heater_side_hca_temp_season = count_consec_vals(group,'heater_side_hca_temp')
        room_side_hca_temp_season = count_consec_vals(group,'room_side_hca_temp')
        hca_units_season = non_zero_vals(group,'hca_units')
        overlap_season = count_overlap_consec_vals(group,cols=['inside_temp','outside_temp','heater_side_hca_temp','room_side_hca_temp','hca_units'],target_col='hca_units')
        row = {
            'room_id': room_id,
            'unit_id': unit_id,
            'building_id': building_id,
            'year': year,
            'inside_temp_season': inside_temp_season,
            'outside_temp_season': outside_temp_season,
            'heater_side_hca_temp_season': heater_side_hca_temp_season,
            'room_side_hca_temp_season': room_side_hca_temp_season,
            'hca_units_season': hca_units_season,
            'overlap_season': overlap_season
        }
        rows.append(row)
    season_counts = pd.DataFrame(rows)
    season_summary = season_counts.groupby(['room_id','unit_id','building_id']).agg({
        'inside_temp_season':'sum',
        'outside_temp_season':'sum',
        'heater_side_hca_temp_season':'sum',
        'room_side_hca_temp_season':'sum',
        'hca_units_season':'sum',
        'overlap_season':'sum'
    }).reset_index()
    season_summary.rename(columns={
        'inside_temp_season':'n_seasons_inside_temp_measured',
        'outside_temp_season':'n_seasons_outside_temp_measured',
        'heater_side_hca_temp_season':'n_seasons_heater_side_hca_temp_measured',
        'room_side_hca_temp_season':'n_seasons_room_side_hca_temp_measured',
        'hca_units_season':'n_seasons_heaters_used',
        'overlap_season':'n_seasons_complete'
    }, inplace=True)
    return season_summary
    #at least 2 weeks of data should be present for each variable in each season

def main_seasons():
    task = Task.init(project_name='ForeSightNEXT/BaltBest', task_name='Data QA_v2 - Seasons')
    task.set_packages(packages='requirements.txt')
    task.execute_remotely(queue_name="default")
    dataset = Dataset.get(dataset_name='ResampledData', dataset_project='ForeSightNEXT/BaltBest/resampled', dataset_version="0.0.1")
    local_path = dataset.get_local_copy()
    resampled = pd.read_csv(f"{local_path}/resampled_data.csv",index_col = 0)
    resampled['ts'] = pd.to_datetime(resampled['ts'])
    rooms_metadata = pd.read_csv(f"{local_path}/rooms_metadata.csv")
    season_counts = seasons_qa(resampled, rooms_metadata)
    os.makedirs('temp', exist_ok=True)
    out_path = 'temp/data_qa_season_report.csv'
    season_counts.to_csv(out_path, index=False)
    Task.current_task().upload_artifact('data_qa_season_report', artifact_object=season_counts)
    Task.current_task().upload_artifact('data_qa_season_report_csv', artifact_object=out_path)
if __name__ == "__main__":
    #test_dataset()
    main_seasons()