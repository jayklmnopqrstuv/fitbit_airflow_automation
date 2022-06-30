from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.utils.decorators import apply_defaults

from google.api_core.exceptions import Conflict

from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Sequence, Union
import json
import logging
import subprocess
import sys

class TemporaryFiles():
    def __init__(self, num_files):
        self.num_files = num_files
    def __enter__(self):
        self.file_handles = [NamedTemporaryFile() for i in range(self.num_files)]
        return self.file_handles
    def __exit__(self, exc_type, exc_value, tb):
        for fh in self.file_handles:
            fh.close()

class GCSFileJoinTransformOperator(BaseOperator):
    """
    This code modified from:
    https://github.com/apache/airflow/blob/master/airflow/providers/google/cloud/operators/gcs.py

    Copies data from source GCS locations to temporary locations on the
    local filesystem. Runs a transformation on these files as specified by
    the transformation script and uploads the output to a destination bucket.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file.

    :param source_bucket: The source bucket in GCS. (templated)
    :type source_bucket: str
    :param source_objects: The source objects to be retrieved from GCS. (templated)
    :type source_objects: List[str]
    :param destination_bucket: The output bucket to be written to in GCS. (templated)
    :type destination_bucket: str
    :param destination_objects: The output objects to be written in GCS. (templated)
    :type destination_object: List[str]
    :param transform_script: location of the executable transformation script or list of arguments
        passed to subprocess ex. `['python', 'script.py', 10]`. (templated)
    :type transform_script: Union[str, List[str]]
    """

    template_fields = (
        'source_bucket',
        'source_objects',
        'destination_bucket',
        'destination_objects',
        'transform_script',
        'impersonation_chain',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        source_bucket: str,
        source_objects: List[str],
        transform_script: Union[str, List[str]],
        destination_bucket: Optional[str] = None,
        destination_objects: Optional[List[str]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_objects = source_objects
        self.destination_bucket = destination_bucket or self.source_bucket
        self.destination_objects = destination_objects
        self.gcp_conn_id = gcp_conn_id
        self.transform_script = transform_script
        self.output_encoding = sys.getdefaultencoding()
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict) -> None:
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        
        with TemporaryFiles(len(self.source_objects)) as source_files, \
                TemporaryFiles(len(self.destination_objects)) as destination_files:

            for source_object, source_file in zip(self.source_objects, source_files):
                self.log.info(f"Downloading file from {source_object} to {source_file.name}")
                hook.download(
                    bucket_name=self.source_bucket, object_name=source_object, filename=source_file.name
                )

            self.log.info("Starting the transformation")
            cmd = [self.transform_script] if isinstance(self.transform_script, str) else self.transform_script
            cmd += ["--source_files"] + [sf.name for sf in source_files]
            cmd += ["--dest_files"] + [df.name for df in destination_files]

            process = subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True
            )

            self.log.info("Process output:")
            if process.stdout:
                for line in iter(process.stdout.readline, b''):
                    self.log.info(line.decode(self.output_encoding).rstrip())

            process.wait()
            if process.returncode:
                raise AirflowException("Transform script failed: {0}".format(process.returncode))

            self.log.info("Transformation succeeded. Output temporarily located at %s", str([x.name for x in destination_files]))

            for destination_object,destination_file in zip(self.destination_objects,destination_files):
                self.log.info(f"Uploading file from {destination_file.name} to {destination_object}")
                hook.upload(bucket_name=self.destination_bucket,object_name=destination_object,filename=destination_file.name)

            
class GCSToPubsubOperator(BaseOperator):
    """
    Sends data from source GCS location to a GCP pubsub. The source GCS location must be
    a newline separated JSON file, where each line is a single message that will be sent.

    The newline separated JSON will populate the 'data' of the pubsub message.  Any attributes
    can be passed with the attributes argument.  The same attributes will be applied to all 
    pubsub messages.

    :param source_bucket: The source bucket in GCS. (templated)
    :type source_bucket: str
    :param source_object: The source object to be retrieved from GCS. (templated)
    :type source_objects: str
    :param attributes: The pubsub attributes to attach to each message.
    :type attributes: dict(str, str)
    :param topic: The pubsub topic to send the message to.
    :type topic: str
    :param gcs_conn_id: The connection id to use for reading the data from GCS.
    :type gcs_conn_id: str
    :param pubsub_conn_id: The connection it to use for sending the data to pubsub.
    :type pubsub_conn_id: str

    """
    template_fields = (
        'source_bucket',
        'source_object',
        'impersonation_chain',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        source_bucket: str,
        source_object: str,
        attributes: Dict[str, str] = None,
        topic: str,
        gcs_conn_id: str = "google_cloud_default",
        pubsub_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object

        self.topic = topic
        self.attributes = attributes

        self.gcs_conn_id = gcs_conn_id
        self.pubsub_conn_id = pubsub_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: dict) -> None:
        gcs_hook = GCSHook(
                gcp_conn_id=self.gcs_conn_id,
                impersonation_chain=self.impersonation_chain,
                )
        pubsub_hook = PubSubHook(
                gcp_conn_id=self.pubsub_conn_id,
                impersonation_chain=self.impersonation_chain,
                )

        with NamedTemporaryFile() as tmp_file:
            self.log.info(f"Downloading file from {self.source_object} to {tmp_file.name}")
            gcs_hook.download(
                    bucket_name=self.source_bucket,
                    object_name=self.source_object,
                    filename=tmp_file.name,
                )

            # read json lines file
            with open(tmp_file.name) as fp:
                messages = list()
                for line in fp:
                    message = {'data': line.encode('utf-8')}
                    if self.attributes:
                        message['attributes'] = self.attributes
                    messages.append(message)
                    
                pubsub_hook.publish(
                        topic=self.topic,
                        messages=messages,
                        )
