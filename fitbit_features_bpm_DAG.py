"""
# odp_level2_feature_store.fitbit_features_bpm
Calculate Fitbit bpm features for all pim  members.
Features are calculated over a window of the previous 1, 7, 10, and 14 days of data.
The following features are calculated for each window:

* period_start_date - First in the window    
* period_end_date - Last day in the window    
* earliest_bpm_date - First day of data in the window  
* latest_bpm_date - Last day of data in the window  
* mean_bpm - Average BPM rate  
* sd_bpm - Standard deviation of bpm  
* min_bpm - Minimum recorded bpm rate  
* max_bpm - Maximum recorded bpm rate  
* mean_resting_bpm - Average resting heart rate  
* sd_resting_bp - Standard deviation of resting bpm  
* min_resting_bpm -Minimum resting heart rate  
* max_resting_bpm - Maximum resting heart rate  
* min_bpm_fatburn - Minimum fatburn heart rate  
* min_bpm_cardio - Minimum cardio heart rate  
* min_bpm_peak - Minimum peak heart rate  
* min_bpm_outofrange - Minimum out of range heart rate  
* max_bpm_fatburn - Maximum fatburn heart rate  
* max_bpm_cardio - Maximum cardio heart rate  
* max_bpm_peak - Maximum peak heart rate  
* max_bpm_outofrange - Maximum out of range heart rate  
* days - Number of days 
* count_ - The number of data points

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

from airflow_utils.operators import GCSFileJoinTransformOperator

from datetime import datetime, timedelta
from pathlib import Path



# Output tables
bpm_output_table    = "odp_level2_feature_store" + ".fitbit_features_bpm${{ ds_nodash }}"

# Stage tables 
bpm_stage_table     = "odp_level2_feature_store_dev" + ".fitbit_features_data_bpm{{ ds_nodash }}"
rest_stage_table    = "odp_level2_feature_store_dev" + ".fitbit_features_data_rest{{ ds_nodash }}"
hrz_stage_table     = "odp_level2_feature_store_dev" + ".fitbit_features_data_hrz{{ ds_nodash }}"


# Query results to csv files
gcs_dir             = "fitbit_biomarker_features/bpm/{{ ds }}"
bpm_csv             = f"{gcs_dir}/bpm_data*.csv"
rest_csv            = f"{gcs_dir}/rest_data*.csv"
hrz_csv             = f"{gcs_dir}/hrz_data*.csv"

# Output json files
bpm_features_json    = f"{gcs_dir}/bpm_features.json"

# Schema files
bpm_schema_json     = f"{gcs_dir}/bpm_output_schema.json"

repo_dir = Path(__file__).parent.absolute()

# Schedule 11 hours offset from UTC.
schedule_interval="0 14 * * *"

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

with DAG("fitbit_features_bpm",
          default_args=default_args,
          schedule_interval=schedule_interval,
          user_defined_macros=common_macros,
          ) as dag:
          
    dag.doc_md = __doc__
	
	
    bpm_query = BigQueryOperator(
            task_id="bpm_query",
            sql="sql/bpm.sql",
            destination_dataset_table=bpm_stage_table,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            )

    export_to_gcs_1 = BigQueryToCloudStorageOperator(
            task_id="export_bpm_to_gcs",
            source_project_dataset_table=bpm_stage_table,
            destination_cloud_storage_uris=f"gs://{ip_bucket}/{bpm_csv}",
            export_format="CSV",
            )
    
    bpmrest_query = BigQueryOperator(
            task_id="rest_query",
            sql="sql/restbpm.sql",
            destination_dataset_table=rest_stage_table,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            )

    export_to_gcs_2 = BigQueryToCloudStorageOperator(
            task_id="export_rest_to_gcs",
            source_project_dataset_table=rest_stage_table,
            destination_cloud_storage_uris=f"gs://{ip_bucket}/{rest_csv}",
            export_format="CSV",
            )
            
    bpmhrz_query = BigQueryOperator(
            task_id="bpmhrz_query",
            sql="sql/hrzbpm.sql",
            destination_dataset_table=hrz_stage_table,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            )

    export_to_gcs_3 = BigQueryToCloudStorageOperator(
            task_id="export_hrz_to_gcs",
            source_project_dataset_table=hrz_stage_table,
            destination_cloud_storage_uris=f"gs://{ip_bucket}/{hrz_csv}",
            export_format="CSV",
            )
            
                
    feature_calculation = GCSFileJoinTransformOperator(
            task_id="feature_calculation",
            source_objects = [bpm_csv, rest_csv, hrz_csv],
            destination_objects = [bpm_features_json],
            transform_script=["python", f"{repo_dir}/system_files/fitbit_bpm_system.py", "{{ ds }}"],
            retry_delay=timedelta(minutes=5),
            )

    upload_bpm_schema = LocalFilesystemToGCSOperator(
            task_id="upload_bpm_schema",
            src=f"{repo_dir}/schema/bpm_output_schema.json",
            dst=bpm_schema_json,
            )
    
      
    bpm_features_to_gbq = GoogleCloudStorageToBigQueryOperator(
            task_id="bpm_features_to_gbq",
            source_objects=[bpm_features_json],
            source_format="NEWLINE_DELIMITED_JSON",
            destination_project_dataset_table=bpm_output_table,
            schema_object=bpm_schema_json,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            )
            
    
    cleanup_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_gcs",
            prefix=gcs_dir,
            )
    
    cleanup_gbq_1 = BigQueryTableDeleteOperator(
            task_id="cleanup_gbq_bpm",
            deletion_dataset_table=bpm_stage_table
            )
    cleanup_gbq_2 = BigQueryTableDeleteOperator(
            task_id="cleanup_gbq_rest",
            deletion_dataset_table=rest_stage_table
            )
    cleanup_gbq_3 = BigQueryTableDeleteOperator(
            task_id="cleanup_gbq_hrz",
            deletion_dataset_table=hrz_stage_table
            )

    
    bpm_query >> export_to_gcs_1 >>  feature_calculation 
    bpmrest_query >>  export_to_gcs_2 >> feature_calculation
    bpmhrz_query >> export_to_gcs_3 >> feature_calculation
    feature_calculation >> bpm_features_to_gbq >> cleanup_gcs
    upload_bpm_schema >> bpm_features_to_gbq >> [cleanup_gbq_1,cleanup_gbq_2,cleanup_gbq_3]
   
