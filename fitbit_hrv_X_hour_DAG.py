"""
# odp_level2_feature_store.fitbit_features_hrv
Calculate X-hour aggregate heart rate variability features for all pim members.
Features are calculated and derived from the 1h,3h,6h and 12h from raw bpm values.

* hrv_type - event type, 1h, 3h, 6h and 12 h
* start_time_local - start datetime of X hour time period
* end_time_local - end datetime of X hour time period
* record_count - The number of data points within X hours. X=1,3,6 and 12 
* bpm_mean - average bpm value within X hours (i.e. between start datetime and end datetime)
* bpm_stddev - standard deviation of bpm value within X hour (i.e. between start datetime and end datetime)
* bpm_min - min bpm value of standard deviation of bpm value within X hour (i.e. between start datetime and end datetime)
* bpm_max - max bpm value of standard deviation of bpm value within X hour (i.e. between start datetime and end datetime)
* beat_interval_ms_mean - Average beat interval (i.e. 60000/bpm) within X hour (i.e. between start datetime and end datetime)
* beat_interval_ms_stddev -Standard deviation from beat interval (i.e. 60000/bpm) within X hour (i.e. between start datetime and end datetime)
* beat_interval_ms_min - min beat interval (i.e. 60000/bpm) within X hour (i.e. between start datetime and end datetime)
* beat_interval_ms_max - max beat interval (i.e. 60000/bpm) within X hour (i.e. between start datetime and end datetime) 
* beat_interval_ms_rmssd - Estimated root mean square of successive differences between normal heartbeats within X hour (i.e. between start datetime and end datetime) 
* geom_poincare_sd1 - Poincare index S1 within X hour (i.e. between start datetime and end datetime). The standard deviation
measured along the minor axis of the Poincare ellipse is called S1, and is a measure of short term variability.
* geom_poincare_sd2 - Poincare index S2 within X hour (i.e. between start datetime and end datetime). The standard deviation
measured along the major axis of the Poincare ellipse is called S2, and is a measure of long term variability.
* tinn - Baseline width of the minimum square difference triangular interpolation of the highest peak of the histogram of all NN intervals within X hour (i.e. between start datetime and end datetime)  
* rrtri - triangular index within X hour (i.e. between start datetime and end datetime)
* sdann - Standard deviation of the averages of NN intervals in all 5 min segments of the entire recording within X hour (i.e. between start datetime and end datetime) 
"""

from airflow_utils.macros import common_macros
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
#from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pydata_google_auth as pgauth
import sys
import os
from pathlib import Path
from google.cloud import bigquery
repo_dir = Path(__file__).parent.absolute()
sys.path.append(str(repo_dir))
#creds = pgauth.get_user_credentials(['https://www.googleapis.com/auth/cloud-platform'])


default_args = {
    "owner": "Jay Gapuz",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 1),
    "email_on_failure": True,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    "use_legacy_sql": False,
    "write_disposition": 'WRITE_TRUNCATE',
    "sla": timedelta(minutes=30)
} 


# def cleanup(tables, **kwargs):
#     tasks = list()
#     for t in tables:
#         tasks.append(BigQueryTableDeleteOperator(task_id=f"Cleanup-{t}", 
#                                                  deletion_dataset_table=t))
#     return tasks


with DAG("fitbit_hrv_x_hour_features",
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          user_defined_macros=common_macros,
          ) as dag:
          
    dag.doc_md = __doc__
    
#     hrv_1h = BigQueryOperator(
#             task_id="hrv_1h",
#             sql="./sql/hrv_1h.sql",
#             destination_dataset_table="odp_level2_feature_store_dev.fitbit_features_hrv_1h",
#             time_partitioning=None,
#             retry_delay=timedelta(minutes=30),
#             task_concurrency=1
#             )

#     hrv_3h = BigQueryOperator(
#             task_id="hrv_3h",
#             sql="./sql/hrv_3h.sql",
#             destination_dataset_table="odp_level2_feature_store_dev.fitbit_features_hrv_3h",
#             time_partitioning=None,
#             retry_delay=timedelta(minutes=30),
#             task_concurrency=1
#             )

#     hrv_6h = BigQueryOperator(
#             task_id="hrv_6h",
#             sql="./sql/hrv_6h.sql",
#             destination_dataset_table="odp_level2_feature_store_dev.fitbit_features_hrv_6h",
#             time_partitioning=None,
#             retry_delay=timedelta(minutes=30),
#             task_concurrency=1
#             )

#     hrv_12h = BigQueryOperator(
#             task_id="hrv_12h",
#             sql="./sql/hrv_12h.sql",
#             destination_dataset_table="odp_level2_feature_store_dev.fitbit_features_hrv_12h",
#             time_partitioning=None,
#             retry_delay=timedelta(minutes=30),
#             task_concurrency=1
#             )    
    
    combine_all = BigQueryOperator(
            task_id="combine_all",
            sql="./sql/hrv_x_hour_1_3_6_12.sql",
            destination_dataset_table="odp_level2_feature_store.fitbit_features_x_hour_hrv${{ ds_nodash }}",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            task_concurrency=1
            )    
     
    
    
