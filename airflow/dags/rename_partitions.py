from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rename_partitions_dag',
    default_args=default_args,
    description='A DAG to rename partitions in Oracle database',
    schedule_interval='0 0 * * *', # run at midnight every day
)

rename_partitions = OracleOperator(
    task_id='rename_partitions_task',
    oracle_conn_id='oracle_neobank',
    sql='BEGIN rename_partitions(); END;',
    dag=dag,
)

rename_partitions
