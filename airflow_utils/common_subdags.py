from airflow_utils.operators import GCSToPubsubOperator
from airflow_utils import env, ip_bucket, ip_feature_store, ip_workspace
  
from airflow import DAG
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow_utils.operators import GCSFileJoinTransformOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from bot_utils.datapull_utils import pim_filter_sql

from datetime import timedelta
from pathlib import Path
import glob
import re

class myGCSFileTransformOperator(GCSFileTransformOperator):
    template_fields = ("source_bucket", "source_object","destination_bucket",
                       "destination_object", "transform_script")



def send_cloud_event_messages(
        parent, 
        default_args=None, 
        dataset_table=None, 
        pubsub_conn_id=None, 
        pubsub_topic=None,
        cloud_event_source=None,
        cloud_event_type=None,
        cloud_event_project=None,
        **kwargs):

    gcs_prefix = f"{parent}/send_cloud_event/"+"{{ds}}/"
    input_path = f"{parent}/send_cloud_event/"+"{{ds}}/input_messages.json"
    output_path = f"{parent}/send_cloud_event/"+"{{ds}}/output_messages.json"
    thisfile_dir = Path(__file__).parent.absolute()

    dag_subdag = DAG(
            parent,
            default_args=default_args,
            **kwargs,
            )

    gbq_to_gcs = BigQueryToCloudStorageOperator(
            task_id=f"{parent}.gbq_to_gcs",
            source_project_dataset_table=dataset_table,
            destination_cloud_storage_uris=f"gs://{ip_bucket}/"+input_path,
            export_format="NEWLINE_DELIMITED_JSON",
            dag=dag_subdag,
            )

    # wrap in cloud event
    wrap_cloud_event = myGCSFileTransformOperator(
            task_id=f"{parent}.wrap_in_cloud_event",
            source_bucket=ip_bucket,
            source_object=input_path,
            destination_object=output_path,
            transform_script=[
                "python", f"{thisfile_dir}/wrap_cloud_event.py",
                cloud_event_source, cloud_event_type, cloud_event_project],
            dag=dag_subdag,
            )

    gcs_to_pubsub = GCSToPubsubOperator(
            task_id=f"{parent}.gcs_to_pubsub",
            pubsub_conn_id=pubsub_conn_id,
            source_bucket=ip_bucket,
            source_object=output_path,
            topic=pubsub_topic,
            dag=dag_subdag,
            )

#    cleanup_gcs = GCSDeleteObjectsOperator(
#            task_id=f"{parent}.cleanup_gcs",
#            bucket_name=ip_bucket,
#            prefix=gcs_prefix,
#            dag=dag_subdag,
#            )

    gbq_to_gcs >> wrap_cloud_event >> gcs_to_pubsub # >> cleanup_gcs
    return dag_subdag


def cleanup(deletion_dataset_tables, dag, dag_id=None, **kwargs):
    """
    In the event that we would like to delete multiple tables from BigQuery,
    this function is useful such that we only input a list of the table
    locations in BigQuery, and the function will handle task-naming and
    the deletions of these tables.
    
    INPUTS:
        deletion_dataset_tables <list>: list of table locations in BigQuery,
            whether table partitions, or the whole table itself. Note that
            deleting table partitions will not delete the table itself even if
            you have deleted all partitions.
        dag <DAG object>: the DAG object to which the BigQueryTableDeleteOperator
            tasks are to be attached to
        dag_id <str>: preset to None, this is to be used for scenarios when
            the DAG object to which the BigQueryTableDeleteOperator is to be attached
            to is a subdag. dag_id should have the structur "parent_dag.child_dag"
    OUTPUT:
        tasks <list>: a list of BigQueryTableDeleteOperator objects
    """
    tasks = list()
    for t in deletion_dataset_tables:        
        # if table can be found in <1>.<2>.<3>, last string is <3>
        # periods inside {{ }} are not considered as delimeters
        last_string = re.findall(r'(?:[^.{{]|\{{[^}}]*\}})+', t)[-1]
        # if the last string contains {{ ** }}[$_-] or [$_-]?{{ ** }}
        # they are removed
        extracted = re.sub(r'(\{\{[^}}]*\}}[$_-]|[$_-]?\{\{[^}}]*\}})', '', last_string)
        
        tasks.append(BigQueryTableDeleteOperator(            
            task_id=f"{dag_id+'.'}" if dag_id else '' + "cleanup_gbq_" + extracted, 
            deletion_dataset_table=t,
            dag=dag,
            **kwargs))
        
    return tasks

  
def get_file_abspath(repo_dir, filename):
    return glob.glob(str(repo_dir) + f"/**/{filename}", recursive = True)[0]
    

def extract_transform_load_data(
        dag_id, 
        repo_dir,
        sql_scripts,
        python_script, 
        output_schema, 
        output_table,
        specific_default_args=None,
        pim_file=None,
        **kwargs):
    """
    Function to abstract common extract-load-transform workflow. 
    Runs the sql script(s) in parallel, passes the data to python for transformation by running the python script,
    and then uploads to GBQ based on the provided schema.
    
    Args:
        dag_id (str): The id of the DAG. If the function is used as a SubDag, 
            dag_id should be prefixed by its parent and a dot i.e. parent_dag_id.subdag_id.
            Else, dag_id.      
        repo_dir (str): Parent directory containing the file where the function is called
        sql_scripts (List[str]): List of filename(s) of the sql scripts that are used to create the stage table(s)       
        python_script (str): Filename of the transform script .py file     
        output_schema (str): Filename of the output schema .json file     
        output_table (str): Output table name. The table will be stored in {ip_feature_store}.{output_table}      
        specific_default_args (dict): Other usecase-specific constructor parameters for initializing operators. Default=None.
        pim_file (str): Filename of the pim filter sql script. Default=None.
            
    Returns:    
        dag: DAG object
        
    Notes:
        1. All sql_scripts, python_script and output_schema should be within repo_dir. 
           This function will automatically look for these files in repo_dir including in subdirectories. 
           Thus, filenames must be unique.
    """

    
    default_args = {
        "depends_on_past": False,
        "email_on_failure": env == 'prod',
        "email_on_retry": False,
        "retries": 3 if env == 'prod' else 0,
        "retry_delay": timedelta(minutes=5),
        "use_legacy_sql": False,
        "bucket": ip_bucket,
        "bucket_name": ip_bucket,
        "source_bucket": ip_bucket,
        "destination_bucket": ip_bucket,
        "write_disposition":"WRITE_TRUNCATE"
    }
    if specific_default_args:
        default_args.update(specific_default_args)

    
    gcs_dir = f"{dag_id}/{output_table}/"+"{{ ds }}" 
    output_schema_json = f"{gcs_dir}/{output_schema}"
    output_json = f"{gcs_dir}/{output_table}.json"
    output_table = f"{ip_feature_store}.{output_table}$" + "{{ ds_nodash }}"

    if pim_file:
        sql_source_paths = None
    else:
        sql_source_paths = []
        for sql_script in sql_scripts:
            p = '/'.join(get_file_abspath(repo_dir, sql_script).split('/')[:-1])
            if p not in sql_source_paths: 
                sql_source_paths.append(p)
    
    python_script_abspath = get_file_abspath(repo_dir, python_script)
    output_schema_abspath = get_file_abspath(repo_dir, output_schema)

    
    dag = DAG(
            dag_id,
            default_args=default_args,
            template_searchpath=sql_source_paths,
            **kwargs,
            )
    
    task_dag_id = f"{dag_id+'.' if '.' in dag_id else ''}"
    
    stage_table_list = []
    source_data_csv_list = []
    source_data_tasks = []

    for sql_script in sql_scripts:
        sql_name = sql_script.replace('.sql','')
        stage_table = f"{ip_workspace}.{dag_id.split('.')[0]}_{sql_name}" + "{{ ds_nodash }}"
        stage_table_list.append(stage_table)
        source_data_csv = f"{gcs_dir}/source_data_{sql_name}.csv"
        source_data_csv_list.append(source_data_csv)


        source_query = BigQueryOperator(
                task_id=task_dag_id + f"source_query-{sql_name}",
                sql=pim_filter_sql(get_file_abspath(repo_dir, sql_script), get_file_abspath(repo_dir, pim_file)) if pim_file else sql_script,
                destination_dataset_table=stage_table,
                dag=dag,
                )

        bq_to_cs = BigQueryToCloudStorageOperator(
                task_id=task_dag_id + f"export_query_data_to_gcs-{sql_name}",
                source_project_dataset_table=stage_table,
                destination_cloud_storage_uris=f"gs://{default_args['bucket']}/{source_data_csv}",
                export_format="CSV",
                dag=dag,
                )
        
        source_data_tasks.append(source_query >> bq_to_cs)
    
    
    python_transform = GCSFileJoinTransformOperator(
            task_id=task_dag_id + "python_transform",
            source_objects=source_data_csv_list,
            destination_objects=[output_json],
            transform_script=["python", python_script_abspath, "{{ ds }}"],
            dag=dag,
            )

    upload_schema = LocalFilesystemToGCSOperator(
            task_id=task_dag_id + "upload_schema",
            src=output_schema_abspath,
            dst=output_schema_json,
            dag=dag,
            )
    
    output_to_gbq = GoogleCloudStorageToBigQueryOperator(
            task_id=task_dag_id + "output_to_gbq",
            source_objects=[output_json],
            source_format="NEWLINE_DELIMITED_JSON",
            destination_project_dataset_table=output_table,
            schema_object=output_schema_json,
            time_partitioning = {"field": "ds"},
            dag=dag,
            )
    
    cleanup_gcs = GCSDeleteObjectsOperator(
            task_id=task_dag_id + "cleanup_gcs",
            prefix=gcs_dir,
            dag=dag,
            )
    
    cleanup_gbq = cleanup(
           dag_id=dag_id,
           deletion_dataset_tables=stage_table_list,
           dag=dag)

    source_data_tasks >> python_transform >> output_to_gbq
    upload_schema >> output_to_gbq >> cleanup_gcs
    output_to_gbq >> cleanup_gbq
    
    return dag
