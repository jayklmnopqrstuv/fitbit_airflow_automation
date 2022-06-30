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
    


def calc_hrv_metrics(period_hrv):
    '''
    Calculate the HRV features derived from Fitbit (bpm) 
    period_hrv : dataframe containing the sleep stages bpm values
    '''
    rmssd_df = aggregate_table(
        period_hrv,
        "beat_interval_ms_rmssd",
        "participant_id",
        [bpm_features.mean_rmssd]
    )  
        
    sdrr_df = aggregate_table(
        period_hrv,
        "beat_interval_ms_stddev",
        "participant_id",
        [bpm_features.mean_sdrr]
    )    

    hrv_df = join_tables([rmssd_df,sdrr_df])
    hrv_df = hrv_df.replace({np.nan: None})
    
    return hrv_df
    
    
def generate_hrv_summary(hrv_tbl, date):
    '''
    Main method to summarize hrv features
    '''
    #We will be summarizing it per day
    hrv_agg = calc_hrv_metrics(hrv_tbl)
    
    return hrv_agg
   
    
    
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
    
   
    # HRV Features

    logging.info(f'---Summarizing HRV Features---')
    logging.info(f'Reading input data')
    hrv_tbl = pd.read_csv(src_files[0])
    
    logging.info(f'Preprocessing Fitbit HRV...')
    hrv_tbl = preprocess(hrv_tbl)
    
    logging.info(f'Calculating hrv features...')
    data_summary =  generate_hrv_summary(hrv_tbl, date)
    
    logging.info(f'Saving data to json file...')
    save_data(data_summary, dest_files[0], date)
    

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

    
    
    
  
