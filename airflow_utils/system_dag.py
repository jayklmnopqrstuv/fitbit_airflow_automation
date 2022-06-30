from airflow_utils.macros import common_macros
from airflow_utils import env

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import os
from datetime import datetime, timedelta

default_args = {
    "owner": "Andrew Brown",
    "start_date": datetime(2020, 10, 27),
    "email": ["abrown1@savvysherpa.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

cmd_template = """
set -ex
cd /opt/airflow/dags/{repo}
git fetch origin master
git reset --hard origin/master
git submodule update --init --recursive
"""

if env == 'prod':
    with DAG("System_Maintenance_DAG",
            default_args=default_args,
            schedule_interval=timedelta(minutes=10),
            user_defined_macros=common_macros,
            catchup=False,
            ) as dag:

        # get all dag directories
        update_repos = list()
        for repo in next(os.walk('/opt/airflow/dags/'))[1]:
            t = BashOperator(
                    bash_command=cmd_template.format(repo=repo),
                    task_id='update_' + repo,
                    sla=timedelta(minutes=1),
                    )
            update_repos.append(t)

        update_repos
