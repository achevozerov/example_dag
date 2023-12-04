from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import timedelta
from source.alerting import set_errors_status, start_dag, finish_dag, zabbix_params

import pendulum


default_args = {
    'owner': 'achevozerov',
    'depends_on_past': False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    'on_failure_callback': set_errors_status
}

with DAG(
    dag_id="amplitude_etl",
    default_args=default_args,
    start_date=pendulum.datetime(2022, 5, 11, tz="UTC"),
    schedule_interval="0 3,9,15,21 * * *",
    catchup=True
) as dag:

    from source.amplitude import wrapper_amplitude_streaming

    with TaskGroup(group_id='rdl_amplitude') as rdl_task_group:
        amplitude_steaming_etl = PythonOperator(
            task_id="amplitude_streaming",
            python_callable=wrapper_amplitude_streaming,
            op_args=["{{ ts_nodash }}", Variable.get("amplitude_temp_path"), Variable.get("amplitude_api_key"), Variable.get("amplitude_api_secret_key")]
        )

        delete_temp_data = BashOperator(
            task_id='delete_temp_amplitude',
            bash_command='cd /root/airflow/dags/temp && rm amplitude.json'
        )

    with TaskGroup(group_id='cdl_amplitude') as tg2:
        dbt_1 = BashOperator(
            task_id='dbt_temp_tables',
            bash_command='source /root/airflow/dags/dbt/bin/activate; cd /root/airflow/dags/dbt/au4/dbt_hoff; dbt run --select models/temp_tables/amplitude'
        )
        dbt_2 = BashOperator(
            task_id='dbt_data_vault',
            bash_command='source /root/airflow/dags/dbt/bin/activate; cd /root/airflow/dags/dbt/au4/dbt_hoff; dbt run --select models/data_vault/amplitude'
        )
        dbt_3 = BashOperator(
            task_id='dbt_data_marts',
            bash_command='source /root/airflow/dags/dbt/bin/activate; cd /root/airflow/dags/dbt/au4/dbt_hoff; dbt run --select models/data_marts/amplitude'
        )

    start = PythonOperator(
        task_id="start_dag",
        python_callable=start_dag,
        op_args=["amplitude_etl", zabbix_params]
    )

    finish = PythonOperator(
        task_id="finish_dag",
        python_callable=finish_dag,
        op_args=["amplitude_etl", zabbix_params],
        trigger_rule="all_done"
    )

start >> amplitude_steaming_etl >> delete_temp_data >> dbt_1 >> dbt_2 >> dbt_3 >> finish