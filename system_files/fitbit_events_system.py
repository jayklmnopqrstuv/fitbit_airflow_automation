import pandas as pd
import json
from datetime import datetime
import logging
from functools import reduce
import aggregate_funcs.bpm_features as bpm_features
import numpy as np



def preprocess(df):
    '''
    Preprocessing function
    return "datetime_local" as the index of the dataframe
    '''
    # Add if needed along the way
    #df = sleeps.preprocess.clean_pipeline(df, method='linear',
    #                                      verbose=True,
    #                                      print_func=logging.info,
    #                                      min_bpm = 40, max_bpm = 200)
    # remove duplicates
    df=df.drop_duplicates()    
    return df.set_index(pd.DatetimeIndex(df['datetime_local'])).sort_index()

def rename_cols(df,newcols):
    '''
    Rename the columns of a dataframe
    df: pandas dataframe
    newcols = dictionary containing old column names and
              new column names
    '''
    
    return df.rename(columns = newcols)

def aggregate_table(df,agg_col,group_col,metrics):
    '''
    Aggregate the values of the dataframe column based from
        the defined metrics
    df: pandas dataframe
    agg_col(str): column name that will be aggregated
    group_col (str/list): determine the columns for the groupby
    metrics: list of aggregation methods
    '''
 
    if isinstance(group_col, str):
        df =  df[[group_col,agg_col]].groupby(group_col).agg(metrics)
    else:
        df = df[group_col + [agg_col]].groupby(group_col).agg(metrics)
    #flatten multilevel index from aggregation
    if isinstance(df.keys(), pd.core.indexes.multi.MultiIndex):
        df.columns = df.columns.droplevel()
    return df


def join_tables(df_list):
    '''
    Join multiple tables by `participant_id`
    df_list = list of pandas dataframe with "participant_id" col
    '''
    df_temp = reduce(
        lambda  left,right: pd.merge(
            left,right,on=['participant_id'],
            how='outer'
        ), 
        df_list)
        
    return df_temp
    
    
def calc_event_metrics(period_events):
    '''
    Calculate the event bpm values 
    period_events : dataframe containing the sleep stages bpm values
    '''
    bpm_metrics = [ 
        bpm_features.mean_bpm,
        bpm_features.sd_bpm,
        bpm_features.min_bpm,
        bpm_features.max_bpm,
        bpm_features.count_
    ]
    
    stage_df = pd.DataFrame(
        period_events['participant_id'].unique(),
        columns = ["participant_id"]
    )
    
    # generate a fake dataframe to avoid error
    stage_df_long = pd.DataFrame({'participant_id': ['-1'],
                                  'start_time': [None],
                                  'end_time': [None],
                                  'mean_bpm': [None],
                                  'sd_bpm': [None],
                                  'min_bpm': [None],
                                  'max_bpm':[None],                                  
                                  'event': 'fake'}).set_index(['participant_id','start_time','end_time'])
    
    stage_df_wide = pd.DataFrame(
        period_events['participant_id'].unique(),
        columns = ["participant_id"]
    )
    
    event_stages = [
        'wake', 'light', 'deep', 'rem',
        'awake','asleep','restless',
        'bedtime_start','bedtime_end',
        'meal_start','peak_postprandial_start',
        'meal_return_baseline'
    ]
    
    for key in event_stages:
        # get long-format event table
        stage_bpm = aggregate_table(
            period_events[period_events.event == key],
            "bpm",
            ["participant_id","start_time","end_time"],
            bpm_metrics
        )
        stage_bpm["event"] = key      
        stage_df_long = stage_df_long.append(stage_bpm)
        
        # get wide-format event table
        stage_bpm = aggregate_table(
            period_events[period_events.event == key],
            "bpm",
            "participant_id",
            bpm_metrics
        )
        new_name = {
            'mean_bpm': 'mean_bpm_' + key,
            'sd_bpm': 'sd_bpm_' + key,
            'max_bpm':'max_bpm_' + key,
            'min_bpm': 'min_bpm_' + key,
            'count_' : 'count_' + key
            }
        stage_bpm = rename_cols(stage_bpm, new_name)
        stage_df_wide = join_tables([stage_df_wide,stage_bpm])
        
   
    stage_df_long = stage_df_long.replace({np.nan: None})
    stage_df_long = stage_df_long.reset_index()
    stage_df_long = stage_df_long.set_index("participant_id")
    #Json format doesnt accept Timestamp
    stage_df_long["start_time"] = stage_df_long["start_time"].astype(str)
    stage_df_long["end_time"] = stage_df_long["end_time"].astype(str)
    
    stage_df_wide = stage_df_wide.replace({np.nan: None})
    stage_df_wide = stage_df_wide.set_index("participant_id")
        
    return stage_df_long, stage_df_wide
    

def generate_event_summary(event_tbl):
    '''
    Main method to summarize bpm features of each daily event
    Note: We only have sleep stages data for now; we'll incorporate meal events
    '''

    event_agg = calc_event_metrics(event_tbl)
    
    return event_agg
    
    
    
def save_data(df, dest_file, date, multilevel_idx = False):
    '''
    Save the data in json format
    df = dataframe of the summarized file
    dest_file = location and filename of the output
    date = summary date
    multilevel_idx: boolean to specify if dataframe has multilevel index
    '''

    batch_time = datetime.utcnow().isoformat('T')

    with open(dest_file, 'w') as dest:
        for ppt, data in df.iterrows():
            if not multilevel_idx:
                ppt_dict = data.to_dict()
            else: 
                ppt_dict = data.unstack().to_dict('index')
            ppt_dict['pim_id'] = ppt
            ppt_dict['ds'] = date
            ppt_dict['batch_time'] = batch_time
            json.dump(ppt_dict, dest)
            dest.write('\n')


def main(date, src_files, dest_files):
    

    # Sleep Event BPM Features
    logging.info(f'---Summarizing Sleep Event BPM Features---')
    
    logging.info(f'Reading input data')
    event_tbl = pd.read_csv(src_files[0])
    
    logging.info(f'Preprocessing Fitbit BPM...')
    event_tbl = preprocess(event_tbl)
    
    logging.info(f'Calculating event bpm features...')
    data_summary = generate_event_summary(event_tbl)

    logging.info(f'Saving data (daily event calculation - long format) to json file...')
    data_long=data_summary[0][data_summary[0].event!='fake']
    save_data(data_long, dest_files[0], date)
    
    logging.info(f'Saving data (daily calculation - wide format ) to json file...')
    save_data(data_summary[1], dest_files[1], date)    

    logging.info('Done!')
  

if __name__ == "__main__":
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("date",
            help="Date to calculate event features for (format YYYY-MM-DD)")
    parser.add_argument("--source_files",
        nargs="*",
        help="Path to source data file")
    parser.add_argument("--dest_files",
        nargs="*",
        help="Path to destination data file")
    args = parser.parse_args()

    main(args.date, args.source_files, args.dest_files)
