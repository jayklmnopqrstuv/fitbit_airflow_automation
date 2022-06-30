from airflow_utils.macros import common_macros
from airflow_utils.operators import GCSFileJoinTransformOperator
from airflow_utils.operators import GCSToPubsubOperator
from airflow_utils.subdag import get_subdag

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.external_task_sensor import ExternalTaskMarker

from datetime import datetime, timedelta
from pathlib import Path

repo_dir = Path(__file__).parent.absolute()

ip_bucket = "bkt_level2_ip_dev"
default_args = {
    "owner": "Andrew Brown",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 13),
    "email": ["abrown1@savvysherpa.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "use_legacy_sql": False,
    "bucket": ip_bucket,
    "bucket_name": ip_bucket,
    "source_bucket": ip_bucket,
    "destination_bucket": ip_bucket,
}

with DAG("Test_DAG",
        default_args=default_args,
        schedule_interval="0 12 * * *",
        user_defined_macros=common_macros,
        ) as dag:

    #### begin multiple file join example
    t1 = BigQueryToCloudStorageOperator(
            task_id="read1",
            source_project_dataset_table="odp_level2_feature_store.T2D_Drug_NDC_List",
            destination_cloud_storage_uris=f"gs://{ip_bucket}/test/"+"{{ ds }}/t1",
            export_format="CSV",
            )
    t2 = BigQueryToCloudStorageOperator(
            task_id="read2",
            source_project_dataset_table="odp_level2_feature_store.T2D_Drug_NDC_List",
            destination_cloud_storage_uris=f"gs://{ip_bucket}/test/"+"{{ ds }}/t2",
            export_format="CSV",
            )
    t3 = BigQueryToCloudStorageOperator(
            task_id="read3",
            source_project_dataset_table="odp_level2_feature_store.T2D_Drug_NDC_List",
            destination_cloud_storage_uris=f"gs://{ip_bucket}/test/"+"{{ ds }}/t3",
            export_format="CSV",
            )

    t4 = GCSFileJoinTransformOperator(
            task_id="join_transform",
            source_object='bla',
            source_objects=[
                "test/{{ ds }}/t1",
                "test/{{ ds }}/t2",
                "test/{{ ds }}/t3",
                ],
            destination_objects=["test/{{ ds }}/output"],
            transform_script=["python", f"{repo_dir}/bot.py", "--date", "{{ ds }}"],
            retry_delay=timedelta(minutes=1),
            )

    cleanup_gcs = GCSDeleteObjectsOperator(
            task_id="cleanup_gcs",
            prefix="test/"+"{{ ds }}",
            )
    
    [t1, t2, t3] >> t4 >> cleanup_gcs

    #### End multiple file join example

    #### Begin cross dag dependency example
    glycemic_features = ExternalTaskSensor(
            task_id="glycemic_features_sensor",
            # this is the string id of the other DAG
            external_dag_id="glycemic_features",
            # this is the string id of the specific task of the other dag
            external_task_id="features_to_gbq",
            # this is how much earlier the other dag ran before this one
            execution_delta=timedelta(hours=1),
            # max time to wait for the other task to be completed
            timeout=300,
            )

    medications = ExternalTaskSensor(
            task_id="medications_sensor",
            external_dag_id="Level2_Individual_Medications",
            external_task_id="Individual_Current_Med",
            execution_delta=timedelta(hours=12),
            timeout=300,
            )

    dummy_child = ExternalTaskSensor(
            task_id="dummy_child",
            external_dag_id="Test_Parent_DAG",
            external_task_id="parent_marker",
            execution_delta=timedelta(hours=2),
            timeout=300,
            )

    join_tables = BigQueryOperator(
            task_id="join_tables",
            sql="join.sql",
            destination_dataset_table="odp_level2_sandbox.ab_glycemic_medications_example",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning={"field": "ds"},
            retry_delay=timedelta(minutes=30),
            )

    join_tables_int_partition = BigQueryOperator(
            task_id="join_tables_int_partition",
            sql="join.sql",
            destination_dataset_table="odp_level2_sandbox.ab_glycemic_medications_example_integer_partition",
            write_disposition="WRITE_TRUNCATE",
            api_resource_configs={
                "query": 
                    {"rangePartitioning": 
                        {"field": "insulin",
                         "range": {"start": 0, "interval": 1, "end": 10}}}},
            retry_delay=timedelta(minutes=30),
            )

    #### End cross dag Dependency example

    #### Begin GBQ to pubsub example

    bq_to_cs = BigQueryToCloudStorageOperator(
            task_id="to_cs_for_pubsub",
            source_project_dataset_table="odp_level2_sandbox.ab_glycemic_medications_example",
            destination_cloud_storage_uris=f"gs://{ip_bucket}/test/"+"{{ ds }}/to_pubsub",
            export_format="NEWLINE_DELIMITED_JSON",
            )

    gcs_to_pubsub = GCSToPubsubOperator(
            task_id="gcs_to_pubsub",
            pubsub_conn_id='gcp-development-00',
            source_object="test/{{ ds }}/to_pubsub",
            topic='tpc_int_dev_level2_mipu_glyc_features',
            )

    #### End GBQ to pubsub example

    [glycemic_features, medications, dummy_child] >> join_tables >> join_tables_int_partition >> bq_to_cs >> gcs_to_pubsub >> cleanup_gcs

with DAG("Test_Parent_DAG",
        default_args=default_args,
        schedule_interval="0 10 * * *",
        user_defined_macros=common_macros,
        ) as dag2:

    parent_marker = ExternalTaskMarker(
            task_id='parent_marker',
            external_dag_id="Test_DAG",
            external_task_id="dummy_child",
            )

    parent_task = DummyOperator(task_id="parent_task")

    parent_marker >> parent_task

with DAG("Subdag_example",
        default_args=default_args,
        schedule_interval="0 10 * * *",
        user_defined_macros=common_macros,
        ) as dag3:

    subdag = SubDagOperator(
        task_id='subdag',
        subdag=get_subdag(
            'Subdag_example',
            "odp_level2_sandbox.ab_glycemic_medications_example",
            'tpc_int_dev_level2_mipu_glyc_features',
            default_args,
            schedule_interval="0 10 * * *",
            ),
        )

    child_task = DummyOperator(task_id="child_task")

    subdag >> child_task

