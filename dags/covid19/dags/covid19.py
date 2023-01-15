# Pipeline
"""
Dag Covid pipeline

### Usage:

1. Must create a connection 'airflow_output' 'File Path' as type with the path for data files.
{"path": "datalake"}

2. Set variables on Airflow UI -> variables, blueprint das variaveis está em
covid19/utils/variables.json

3.Data transformation scripts available at: covid19/utils/preprocess.py

schedules : 
https://crontab.guru/#15_14_1_*_*

"""
from airflow import DAG
from os.path import join
import pendulum
from datetime import timedelta
from covid19.utils.preprocess import transform_raw_data, create_trusted_layer, create_refined_layer
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


# GLOBAL VARS:
RELATIVE_RAW_PATH = 'data/raw/covid19/*.csv'
CONFIRMED_CASES_PATH = Variable.get('CONFIRMED_CASES_PATH')
RECOVERED_PATH = Variable.get('RECOVERED_PATH')
DEATHS_PATH = Variable.get('DEATHS_PATH')
TRUSTED_PATH = Variable.get('TRUSTED_PATH')
REFINED_PATH = Variable.get('REFINED_PATH')
PROCESSED_PATH = Variable.get('PROCESSED_PATH')
CONFIRMED_COL = "total_confirmed"
DEATHS_COL = "total_deaths"
RECOVERED_COL = "total_recovered"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["lucas.angulski@gmail.com"],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="COVID19-transform",
    default_args=default_args,
    description="pipeline for covid data",
    template_searchpath=["/opt/airflow/dags"],
    schedule_interval="15 22 * * *",  # or  @daily
    catchup=False,
    start_date=pendulum.datetime(2022, 12, 22, tz="UTC"),
    tags=["COVID19", "Pyspark", "csv", "trusted", "refined"],
)


check_files = FileSensor(
    task_id="check-files-exists",
    default_args=default_args,
    poke_interval=15,
    timeout=15*2,
    soft_fail=True,
    dag=dag,
    fs_conn_id="airflow_output",
    filepath=RELATIVE_RAW_PATH
)

with TaskGroup(group_id="extract-raw", dag=dag) as tg:

    extract_confirmed_cases = PythonOperator(
        task_id='transform-confirmed',
        default_args=default_args,
        dag=dag,
        python_callable=transform_raw_data,
        op_kwargs={
            "input_path": CONFIRMED_CASES_PATH,
            "output_path": PROCESSED_PATH,
            "variable_name": CONFIRMED_COL
        }
    )

    extract_deaths = PythonOperator(
        task_id='transform-deaths',
        default_args=default_args,
        dag=dag,
        python_callable=transform_raw_data,
        op_kwargs={
            "input_path": DEATHS_PATH,
            "output_path": PROCESSED_PATH,
            "variable_name": DEATHS_COL
        }
    )

    extract_recovered = PythonOperator(
        task_id='transform-recovered',
        default_args=default_args,
        dag=dag,
        python_callable=transform_raw_data,
        op_kwargs={
            "input_path": RECOVERED_PATH,
            "output_path": PROCESSED_PATH,
            "variable_name": RECOVERED_COL
        }
    )

    [extract_confirmed_cases, extract_deaths, extract_recovered]

# trusted layer
trusted_layer = PythonOperator(
    task_id='trusted-layer',
    default_args=default_args,
    dag=dag,
    trigger_rule="all_success",
    python_callable=create_trusted_layer,
    op_kwargs={
        "confirmed_input": join(PROCESSED_PATH, CONFIRMED_COL),
        "death_input": join(PROCESSED_PATH, DEATHS_COL),
        "recovered_input": join(PROCESSED_PATH, RECOVERED_COL),
        "output_path": TRUSTED_PATH
    }
)

# create_refined_layer
refined_layer = PythonOperator(
    task_id='refined-layer',
    default_args=default_args,
    dag=dag,
    trigger_rule="all_success",
    python_callable=create_refined_layer,
    op_kwargs={
        "input_path": TRUSTED_PATH,
        "output_path": REFINED_PATH,
        "columns_schema": {"confirmed": CONFIRMED_COL,
                           "deaths": DEATHS_COL,
                           "recovered": RECOVERED_COL,
                           }
            }
)

check_files >> tg >> trusted_layer >> refined_layer
