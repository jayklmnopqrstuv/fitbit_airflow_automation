"""

# odp_level2_feature_store.fitbit_features_events_detail  
Calculate Fitbit bpm features of each event recorded for all pim members.  
Features are calculated from each recorded daily event.  
Events include:  
(a) sleep stages  
(b) meal events  

Table is in long format with the following columns:  
  
* event - the name of the event  
        (a) wake    
        (b) light    
        (c) deep   
        (d) rem    
        (e) awake    
        (f) asleep    
        (g) restless    
        (h) bedtime_start (10 mins before sleep)    
        (i) bedtime_end (10 mins at the end of sleep event)    
        (j) meal_start (10 mins before the start of meal)    
        (k) meal_return_baseline (10 mins before glucose returned to baseline value)    
        (l) peak_postprandial (10 mins before the onset of peak postprandial glucose event)  
        
* start_time - Beginning time of the recorded event    
* end_time - Finish time of the recorded event   
* mean_bpm - Estimated from average of bpm during sleep/meal event (daily)    
* sd_bpm - Estimated from the standard bpm value during sleep/meal event (daily)    
* min_bpm - Minimum measured bpm value during sleep/meal event (daily)    
* max_bpm - Maximum measured bpm value during sleep/meal event (daily)    
* count_  - Count of bpm entries    

  
# odp_level2_feature_store.fitbit_features_events_daily  
Calculate Fitbit bpm features of all similar events recorded daily for all pim members.  
Features are calculated from all similar daily events. 

Events include:
(a) sleep stages  
(b) meal events  

Table is in wide format with the following columns:   

* mean_bpm_xxx  
* sd_bpm_xxx  
* min_bpm_xxx  
* max_bpm_xxx  

where 'xxx' is any of the ff events:    
rem, deep, light, asleep, wake, bedtime_start, bedtime_end, meal_start, peak_postpandrial  


"""

from airflow_utils.macros import common_macros
from airflow_utils import env, ip_bucket, ip_feature_store, ip_workspace

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow_utils.operators import GCSFileJoinTransformOperator

from datetime import datetime, timedelta
from pathlib import Path



# Output tables
events_output_table_long  = "odp_level2_feature_store" + ".fitbit_features_events_detail${{ ds_nodash }}"
events_output_table_wide  = "odp_level2_feature_store" + ".fitbit_features_events_daily${{ ds_nodash }}"

# Stage tables 
events_stage_table   = "odp_level2_feature_store_dev" + ".fitbit_features_data_events${{ ds_nodash }}"


# Query results to csv files
gcs_dir             = "fitbit_biomarker_features/events/{{ ds }}"
events_csv           = f"{gcs_dir}/events_data*.csv"

# Output json files
events_features_json_long  = f"{gcs_dir}/events_features_long.json"
events_features_json_wide  = f"{gcs_dir}/events_features_wide.json"

# Schema files
events_schema_json_long   = f"{gcs_dir}/events_output_schema_long.json"
events_schema_json_wide   = f"{gcs_dir}/events_output_schema_wide.json"

repo_dir = Path(__file__).parent.absolute()

# Schedule 11 hours offset from UTC.
schedule_interval="0 16 * * *"

default_args = {
    "owner": "Jay Gapuz",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 1),
    "email_on_failure": env != 'prod',
    "email_on_retry": False,
    "retries": 3 if env == 'prod' else 0,
    "retry_delay": timedelta(minutes=5),
    "use_legacy_sql": False,
    "bucket": ip_bucket,
    "bucket_name": ip_bucket,
    "source_bucket": ip_bucket,
    "destination_bucket": ip_bucket,
}

with DAG("fitbit_features_events",
          default_args=default_args,
          schedule_interval=schedule_interval,
          user_defined_macros=common_macros,
          ) as dag:
          
    dag.doc_md = __doc__
    
    meal_events_sensor = ExternalTaskSensor(
              task_id="meal_events_sensor",
              external_dag_id= "meal_events",
              external_task_id="python_transform",
              execution_delta=timedelta(hours=1),
              timeout=600,
              )
    
    events_query = BigQueryOperator(
            task_id="events_query",
            sql="sql/events_bpm.sql",
            destination_dataset_table=events_stage_table,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            )

    export_to_gcs = BigQueryToCloudStorageOperator(
            task_id="export_events_to_gcs",
            source_project_dataset_table=events_stage_table,
            destination_cloud_storage_uris=f"gs://{ip_bucket}/{events_csv}",
            export_format="CSV",
            )
            
    feature_calculation = GCSFileJoinTransformOperator(
            task_id="feature_calculation",
            source_objects = [events_csv],
            destination_objects = [events_features_json_long, events_features_json_wide],
            transform_script=["python", f"{repo_dir}/system_files/fitbit_events_system.py", "{{ ds }}"],
            retry_delay=timedelta(minutes=5),
            )
    
    
    upload_events_schema_long = LocalFilesystemToGCSOperator(
            task_id="upload_events_schema_long",
            src=f"{repo_dir}/schema/events_output_schema_long.json",
            dst=events_schema_json_long,
            )
    
    upload_events_schema_wide = LocalFilesystemToGCSOperator(
            task_id="upload_events_schema_wide",
            src=f"{repo_dir}/schema/events_output_schema_wide.json",
            dst=events_schema_json_wide,
            )
    
    
            
    events_features_long_to_gbq = GoogleCloudStorageToBigQueryOperator(
            task_id="events_features_long_to_gbq",
            source_objects=[events_features_json_long],
            source_format="NEWLINE_DELIMITED_JSON",
            destination_project_dataset_table=events_output_table_long,
            schema_object=events_schema_json_long,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            )   
    
    events_features_wide_to_gbq = GoogleCloudStorageToBigQueryOperator(
            task_id="events_features_wide_to_gbq",
            source_objects=[events_features_json_wide],
            source_format="NEWLINE_DELIMITED_JSON",
            destination_project_dataset_table=events_output_table_wide,
            schema_object=events_schema_json_wide,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            )   
            
            
    
    cleanup_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_gcs",
            prefix=gcs_dir,
            )
    
    cleanup_gbq = BigQueryTableDeleteOperator(
            task_id="cleanup_gbq_events",
            deletion_dataset_table=events_stage_table
            )

    
    events_query >> export_to_gcs >>  feature_calculation >> events_features_long_to_gbq >> events_features_wide_to_gbq >> cleanup_gbq >> cleanup_gcs
    meal_events_sensor >> events_query
    upload_events_schema_long >> events_features_long_to_gbq
    upload_events_schema_wide >> events_features_wide_to_gbq 

   
