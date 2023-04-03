import os
import re
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
#import snowflake.connector

from helpers import export_partition_to_parquet, export_non_partitioned_table_to_parquet, get_high_value


# Global variables
tables_non_partitioned = ['CLIENT', 'ACCOUNT', 'CARD', 'COUNTRY', 'CURRENCY']
tables_partitioned_by_day = ['TRANSACTION']
tables_partitioned_by_month = []

postgres_conn_id = 'postgres_metadata'
oracle_conn_id = "oracle_neobank"
schema_name = 'NEOBANK' 


def flush_database_monitoring_info():
    hook = OracleHook(oracle_conn_id=oracle_conn_id)
    conn = hook.get_conn()

    # Iterate through the list of tables to gather statistics
    flush_database_monitoring_info_query = """
    BEGIN
        DBMS_STATS.FLUSH_DATABASE_MONITORING_INFO;
    END;
    """

    with conn.cursor() as cursor:
        cursor.execute(flush_database_monitoring_info_query)


def get_tab_modifications(**kwargs):
    # Connect to Oracle and fetch data from USER_TAB_MODIFICATIONS
    oracle_hook = OracleHook(oracle_conn_id)
    oracle_conn = oracle_hook.get_conn()

    oracle_query = """
    SELECT TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME, INSERTS, UPDATES, DELETES, TIMESTAMP AS TIMESTAMP_VAL, TRUNCATED, DROP_SEGMENTS
    FROM USER_TAB_MODIFICATIONS
    """


    oracle_data = []
    with oracle_conn.cursor() as oracle_cursor:
        oracle_cursor.execute(oracle_query)
        for row in oracle_cursor.fetchall():
            row = list(row)
            row[6] = row[6].strftime("%Y-%m-%d %H:%M:%S")  # Convert datetime to string
            oracle_data.append(tuple(row))


    # Pass the comparison results to the next task using XCom
    kwargs["task_instance"].xcom_push("tab_modifications", oracle_data)


def save_tab_modifications_snapshot(**kwargs):
    tab_modifications = kwargs["task_instance"].xcom_pull(task_ids="get_tab_modifications", key="tab_modifications")

    # Connect to PostgreSQL and insert data into the snapshot_tab_modifications table
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()

    postgres_insert_query = """
    INSERT INTO snapshot_tab_modifications (
        snapshot_dt, table_owner, table_name, partition_name, subpartition_name, inserts, updates, deletes, "timestamp", truncated, drop_segments)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    snapshot_dt = datetime.now()
    with postgres_conn.cursor() as postgres_cursor:
        for row in tab_modifications:
            postgres_cursor.execute(postgres_insert_query, (snapshot_dt, schema_name,) + tuple(row))
        postgres_conn.commit()


def export_changed_data(**kwargs):
    tab_modifications = kwargs["task_instance"].xcom_pull(task_ids="get_tab_modifications", key="tab_modifications")

    if tab_modifications is None:
        logging.error("No changed tables or partitions results found.")
        return
    
    changed_tables = set()
    changed_partitions = set()

    for row in tab_modifications:
        table_name, partition_name, subpartition_name, inserts, updates, deletes, timestamp_val, truncated, drop_segments = tuple(row)

        if any([inserts, updates, deletes, truncated=='YES', drop_segments]):
            if partition_name is None:
                changed_tables.add(table_name)
            else:
                changed_partitions.add((table_name, partition_name))

    # Export changed tables from tables_non_partitioned
    for table_name in changed_tables:
        if table_name in tables_non_partitioned:
            export_non_partitioned_table_to_parquet(oracle_conn_id, table_name)

    # Export changed partitions
    for table_name, partition_name in changed_partitions:
        # Skip aggregated row
        if partition_name is not None:
            high_value = get_high_value(oracle_conn_id, table_name, partition_name)
            high_value_date = datetime.strptime(high_value, '%Y-%m-%d')
            partition_label = None

            if table_name in tables_partitioned_by_day:
                partition_label = high_value_date.strftime('%Y_%m_%d')
            elif table_name in tables_partitioned_by_month:
                partition_label = high_value_date.strftime('%Y_%m')

            if partition_label:
                export_partition_to_parquet(oracle_conn_id, table_name, partition_name, partition_label)


def upload_parquet_to_snowflake(**kwargs):
    changed_partitions = kwargs["task_instance"].xcom_pull("changed_partitions")

    # Replace with your Snowflake connection details
    conn = snowflake.connector.connect(
        user="your_user",
        password="your_password",
        account="your_account",
        warehouse="your_warehouse",
        database="your_database",
        schema="your_schema",
    )

    for partition_name, last_analyzed in changed_partitions:
        parquet_file_path = os.path.join("path/to/output/parquet", f"{partition_name}.parquet")

        with open(parquet_file_path, "rb") as file:
            # Replace TABLE_NAME with your target Snowflake table name
            query = f"""
            PUT file://{parquet_file_path}
            @%TABLE_NAME
            AUTO_COMPRESS=TRUE
            """

            with conn.cursor() as cursor:
                cursor.execute(query)

            # Replace TABLE_NAME with your target Snowflake table name
            query = f"""
            COPY INTO TABLE_NAME
            FROM @%TABLE_NAME/{os.path.basename(parquet_file_path)}
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = CONTINUE
            """

            with conn.cursor() as cursor:
                cursor.execute(query)

    conn.close()


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "oracle_to_snowflake",
    default_args=default_args,
    description="Export changed partitions from Oracle to Parquet files",
    schedule_interval='0 0 * * *', # run at midnight every day
    catchup=False,
)

# Define the tasks
flush_database_monitoring_info_task = PythonOperator(
    task_id="flush_database_monitoring_info",
    python_callable=flush_database_monitoring_info,
    provide_context=True,
    dag=dag,
)

get_tab_modifications_task = PythonOperator(
    task_id="get_tab_modifications",
    python_callable=get_tab_modifications,
    provide_context=True,
    dag=dag,
)

save_tab_modifications_snapshot_task = PythonOperator(
    task_id="save_tab_modifications_snapshot",
    python_callable=save_tab_modifications_snapshot,
    provide_context=True,
    dag=dag,
)

export_changed_data_task = PythonOperator(
    task_id="export_changed_data",
    python_callable=export_changed_data,
    provide_context=True,
    dag=dag,
)


#upload_to_snowflake_task = PythonOperator(
#    task_id="upload_parquet_to_snowflake",
#    python_callable=upload_parquet_to_snowflake,
#    provide_context=True,
#    dag=dag,
#)

flush_database_monitoring_info_task >> get_tab_modifications_task

get_tab_modifications_task >> save_tab_modifications_snapshot_task
get_tab_modifications_task >> export_changed_data_task ## >> upload_to_snowflake_task
