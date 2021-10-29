from datetime import datetime

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

import sys



sys.path.append('~srp_env/lib/python3.7/site-packages/')

from connectingPipelines import exchanges_src_extract

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "start_date": datetime(2021, 10, 27),

    "retires": 0



}



dag = DAG(

    dag_id="exchanges_DAG",

    default_args=default_args,

    catchup=False,

    schedule_interval="@once"

)



start = DummyOperator(

    task_id="start",

    dag=dag

)



exchanges_src_extract = PythonOperator(

    task_id="exchanges_src_extract",

    dag=dag,

    python_callable=exchanges_src_extract.main

)
