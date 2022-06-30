from airflow_utils.operators import GCSToPubsubOperator

from airflow import DAG
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


ip_bucket = "bkt_level2_ip_dev"

def get_subdag(parent, table, topic, default_args, **kwargs):
    dag_subdag = DAG(
            f"{parent}.subdag",
            default_args=default_args,
            **kwargs,
            )

    bq_to_cs = BigQueryToCloudStorageOperator(
            task_id=f"to_cs_for_pubsub",
            source_project_dataset_table=table, # "odp_level2_sandbox.ab_glycemic_medications_example"
            destination_cloud_storage_uris=f"gs://{ip_bucket}/to_pubsub/"+"{{ ds }}",
            export_format="NEWLINE_DELIMITED_JSON",
            dag=dag_subdag,
            )

    gcs_to_pubsub = GCSToPubsubOperator(
            task_id="gcs_to_pubsub",
            pubsub_conn_id='gcp-development-00',
            source_bucket=ip_bucket,
            source_object="to_pubsub/{{ ds }}",
            topic=topic,  #'tpc_int_dev_level2_mipu_glyc_features',
            dag=dag_subdag,
            )

    cleanup_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_gcs",
            bucket_name=ip_bucket,
            prefix="to_pubsub/"+"{{ ds }}",
            dag=dag_subdag,
            )

    bq_to_cs >> gcs_to_pubsub >> cleanup_gcs
    return dag_subdag
