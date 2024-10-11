#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""

from __future__ import annotations

import datetime
import time

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

with DAG(
    dag_id="python_operator",
    schedule=datetime.timedelta(seconds=30),
    # schedule="* * * * *",
    start_date=pendulum.datetime(2021, 1, 1, 0, 0, 0, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["python", "example3"],
    params={"example_key": "example_value"},
) as dag:
    
    # read com data
    @task(task_id="read_com_task", multiple_outputs=True)
    def read_com_func():
        print("read_com_task")
        return {
            "data": "com_data",
            "wait_times": [1, 3, 10]
        }
    
    @task.branch(task_id="branch_task")
    def branch_func(ti=None):
        xcom_value = ti.xcom_pull(task_ids="read_com_task")["data"]
        if xcom_value is None:
            return "stop_task"
        elif xcom_value == 'com_data':
            return "process_com_task"
        else:
            return None

    
    @task(task_id="process_com_task", multiple_outputs=True)
    def process_com_func(**kwargs):
        print("process_com_task")
        wait_times = kwargs['ti'].xcom_pull(task_ids="read_com_task")['wait_times']
        for wait_time in wait_times:
            print(f"wait_time: {wait_time}")
            time.sleep(wait_time)
        return {
            "data": "adjusted_com_data"
        }
    
    @task(task_id="sql_insert_task")
    def sql_insert_func(**kwargs):
        sql_data = kwargs['ti'].xcom_pull(task_ids="process_com_task")['data']
        print(f"sql_insert_task: {sql_data}")

    @task(task_id="stop_task")
    def stop_func():
        print("stop_task")
    
    process_com_task = process_com_func()
    read_com_func() >> branch_func() >> [ process_com_task, stop_func()]
    process_com_task >> sql_insert_func()

if __name__ == "__main__":
    dag.clear()
    dag.test()
