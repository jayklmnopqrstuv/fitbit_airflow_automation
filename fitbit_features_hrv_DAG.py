"""

# odp_level2_feature_store.fitbit_features_hrv
Calculate heart rate variability features for all pim members.
Features are calculated and derived from the daily bpm values.

* mean_rmssd - Estimated from average of root mean square of successive differences between normal heartbeats (daily)  
* mean_sdrr - Estimated from average of the standard deviation of all the R-R intervals (daily)  
* beat_interval_ms_mean - Average beat interval (i.e. 60000/bpm) within 24 hour (i.e. between start datetime and end datetime)
* beat_interval_ms_stddev - Standard deviation from beat interval (i.e. 60000/bpm) within 24 hour (i.e. between start datetime and end datetime)
* beat_interval_ms_min - min beat interval (i.e. 60000/bpm) within 24 hour (i.e. between start datetime and end datetime)
* beat_interval_ms_max - max beat interval (i.e. 60000/bpm) within 24 hour (i.e. between start datetime and end datetime)
* beat_interval_ms_rmssd - Estimated root mean square of successive differences between normal heartbeats within 24 hour (i.e. between start datetime and end datetime)
* geom_poincare_sd1 - Poincare index S1 within 24 hour (i.e. between start datetime and end datetime). The standard deviation measured along the minor axis of the Poincare ellipse is called S1, and is a measure of short term variability.
* geom_poincare_sd2 - Poincare index S1 within 24 hour (i.e. between start datetime and end datetime). The standard deviation measured along the minor axis of the Poincare ellipse is called S2, and is a measure of short term variability.
* tinn - Baseline width of the minimum square difference triangular interpolation of the highest peak of the histogram of all NN intervals within 24 hour (i.e. between start datetime and end datetime)
* rrtri - triangular index within 24 hour (i.e. between start datetime and end datetime)
* sdann - Standard deviation of the averages of NN intervals in all 5 min segments of the entire recording within 24 hour (i.e. between start datetime and end datetime)

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


# Schedule 14 hours offset from UTC.
schedule_interval="0 14 * * *"

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

with DAG("fitbit_features_hrv_daily",
          default_args=default_args,
          schedule_interval=schedule_interval,
          user_defined_macros=common_macros,
          ) as dag:
          
    dag.doc_md = __doc__

    hrv_daily = BigQueryOperator(
            task_id="calculate_hrv_daily",
            sql="./sql/hrv_all.sql",
            destination_dataset_table="odp_level2_feature_store.fitbit_features_daily_hrv${{ ds_nodash }}",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            task_concurrency=1
            )      

#     hrv_query = BigQueryOperator(
#             task_id="hrv_query",
#             sql="sql/hrv.sql",
#             destination_dataset_table=hrv_stage_table,
#             write_disposition="WRITE_TRUNCATE",
#             time_partitioning={"field": "ds"},
#             retry_delay=timedelta(minutes=30),
#             )

#     export_to_gcs = BigQueryToCloudStorageOperator(
#             task_id="export_hrv_to_gcs",
#             source_project_dataset_table=hrv_stage_table,
#             destination_cloud_storage_uris=f"gs://{ip_bucket}/{hrv_csv}",
#             export_format="CSV",
#             )
            
#     feature_calculation = GCSFileJoinTransformOperator(
#             task_id="feature_calculation",
#             source_objects = [hrv_csv],
#             destination_objects = [hrv_features_json],
#             transform_script=["python", f"{repo_dir}/fitbit_hrv_system.py", "{{ ds }}"],
#             retry_delay=timedelta(minutes=5),
#             )


#     upload_hrv_schema = LocalFilesystemToGCSOperator(
#             task_id="upload_hrv_schema",
#             src=f"{repo_dir}/schema/hrv_output_schema.json",
#             dst=hrv_schema_json,
#             )

#     hrv_features_to_gbq = GoogleCloudStorageToBigQueryOperator(
#             task_id="hrv_features_to_gbq",
#             source_objects=[hrv_features_json],
#             source_format="NEWLINE_DELIMITED_JSON",
#             destination_project_dataset_table=hrv_output_table,
#             schema_object=hrv_schema_json,
#             write_disposition="WRITE_TRUNCATE",
#             time_partitioning={"field": "ds"},
#             )
    
#     cleanup_gcs = GCSDeleteObjectsOperator(
#             task_id="cleanup_gcs",
#             prefix=gcs_dir,
#             )
    

#     cleanup_gbq = BigQueryTableDeleteOperator(
#             task_id="cleanup_gbq_hrv",
#             deletion_dataset_table=hrv_stage_table
#             )
    
    
#     hrv_query >> export_to_gcs >>  feature_calculation >> hrv_features_to_gbq >> cleanup_gbq >> cleanup_gcs 
#     upload_hrv_schema >> hrv_features_to_gbq
