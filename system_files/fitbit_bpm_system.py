import pandas as pd
import json
from datetime import datetime
import logging
from functools import reduce
import numpy as np


# import sys
# import os
# sys.path.append("../")
import aggregate_funcs.bpm_features as bpm_features


DAYS = [1, 7, 10, 14]


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
    
    
def calc_bpm_metrics(period_bpm,period_rest,period_zone):
    '''
    Calculate the bpm features 
    period_bpm: dataframe containing all bpm values
    period_rest: dataframe containing the resting bpm values
    period_zone: dataframe containing the heart zone bpm values
    '''
    bpm_metrics = [ 
        bpm_features.mean_bpm,
        bpm_features.sd_bpm,
        bpm_features.min_bpm,
        bpm_features.max_bpm,
        bpm_features.count_
    ]
    
    # Resting BPM
    rest_metrics = [
        bpm_features.mean_resting_bpm,
        bpm_features.sd_resting_bpm,
        bpm_features.min_resting_bpm,
        bpm_features.max_resting_bpm,
    ]
    
    # Heart Rate Zones (cardio, fatburn, peak, outofrange)
    hrzones_metrics = [
        bpm_features.min_hrzone_bpm,
        bpm_features.max_hrzone_bpm   
    ]
    
    bpm_df = aggregate_table(
        period_bpm,
        "bpm",
        "participant_id",
        bpm_metrics
    )
   
    rest_df = aggregate_table(
        period_rest,
        "resting_heart_rate",
        "participant_id",
        rest_metrics
    )
    #period_zone["heart_rate_zone"] = period_zone["heart_rate_zone"].str.lower() 
    hrzone_df = pd.DataFrame(
        period_zone['participant_id'].unique(),
        columns = ["participant_id"]
    )
    
    for key in ["cardio","fatburn","peak","outofrange"]:

        min_bpm = aggregate_table(
            period_zone[period_zone.heart_rate_zone == key],
            "min_bpm",
            "participant_id",
            hrzones_metrics[0]
        )
        new_name = {'min_bpm':'min_bpm_'+ key}
        min_bpm = rename_cols(min_bpm,new_name)
        
        max_bpm = aggregate_table(
            period_zone[period_zone.heart_rate_zone == key],
            "max_bpm",
            "participant_id",
            hrzones_metrics[1]
        )
        new_name = {'max_bpm':'max_bpm_' + key}
        max_bpm =   rename_cols(max_bpm,new_name)
        
        hrzone_df = join_tables([hrzone_df,min_bpm,max_bpm])
        
    bpm_all_df = join_tables([bpm_df,rest_df,hrzone_df])
    bpm_all_df = bpm_all_df.replace({np.nan: None})
    return bpm_all_df.set_index("participant_id")


    
def generate_bpm_summary(bpm_tbl, rest_tbl, hrz_tbl, days_list, date):
    '''
    Main method to summarize bpm features 
    bpm_tbl = dataframe containing all bpm values with "datetime_local" as index
    rest_tbl = dataframe containing resting bpm values with "datetime_local" as index
    hrz_tbl = dataframe containing all HR zones bpm values with "datetime_local" as index
    days_list = list of day intervals
    date = summary date
    '''
    final_data = list()
    for days in days_list:
        period_end = date
        period_start = str((pd.Timestamp(period_end) - pd.Timedelta(days = days-1)).date())
        bpm_window = bpm_tbl.loc[period_start:period_end]
        if days == 1 and len(bpm_window.index) == 0:
            raise ValueError(f'No data collected for {date}.')
        rest_window = rest_tbl.loc[period_start:period_end]
        hrz_window = hrz_tbl.loc[period_start:period_end]
        bpm_agg = calc_bpm_metrics(bpm_window,rest_window,hrz_window)
        bpm_agg['days'] = days 
        bpm_agg['period_start_date'] = period_start
        bpm_agg['period_end_date'] = period_end
        bpm_window.index = bpm_window.index.map(str)
        bpm_dates = aggregate_table(
                        bpm_window["participant_id"].reset_index(),
                        "datetime_local",
                        "participant_id",
                        [bpm_features.earliest_bpm_date,bpm_features.latest_bpm_date]
                    )
        # get only the date component
        bpm_dates['earliest_bpm_date'] = bpm_dates['earliest_bpm_date'].str.split(' ').str[0]
        bpm_dates['latest_bpm_date'] = bpm_dates['latest_bpm_date'].str.split(' ').str[0]
        bpm_agg = join_tables([bpm_agg,bpm_dates])
        final_data.append(bpm_agg)
 
    final_df = pd.concat(final_data,axis = 1,keys = ['period_'+str(d)+'d' for d in days_list]) 
    return final_df.where(final_df.notnull(), None)


    
    
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
    
    # BPM Features
    
    logging.info(f'---Summarizing BPM Features---')
    
    logging.info(f'Reading input data')
    bpm_tbl = pd.read_csv(src_files[0])
    rest_tbl = pd.read_csv(src_files[1])
    hrz_tbl = pd.read_csv(src_files[2])
    
    logging.info(f'Preprocessing Fitbit BPM...')
    bpm_tbl = preprocess(bpm_tbl)
    rest_tbl = preprocess(rest_tbl)
    hrz_tbl = preprocess(hrz_tbl)
    
    logging.info(f'Calculating bpm features...')
    data_summary =  generate_bpm_summary(bpm_tbl, rest_tbl, hrz_tbl, DAYS, date)   

    logging.info(f'Saving data to json file...')
    save_data(data_summary, dest_files[0], date, multilevel_idx = True)
    
    logging.info("Done")

    
    
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

    
    
    
  
