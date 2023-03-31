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


# Global variables
tables_non_partitioned = ['CLIENT', 'ACCOUNT', 'CARD', 'COUNTRY', 'CURRENCY']
tables_partitioned_by_day = ['TRANSACTION']
tables_partitioned_by_month = []

postgres_conn_id = 'postgres_metadata'
oracle_conn_id = "oracle_neobank"
schema_name = 'NEOBANK' 


def gather_statistics():
    hook = OracleHook(oracle_conn_id=oracle_conn_id)
    conn = hook.get_conn()

    # Combine all table lists
    all_tables = tables_non_partitioned + tables_partitioned_by_day + tables_partitioned_by_month

    # Iterate through the list of tables to gather statistics
    gather_stats_query = """
    BEGIN
      DBMS_STATS.GATHER_TABLE_STATS(ownname => :schema_name, tabname => :table_name);
    END;
    """

    for table_name in all_tables:
        with conn.cursor() as cursor:
            cursor.execute(gather_stats_query, (schema_name, table_name))


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


def create_snapshot_all_tab_modifications():
    # Connect to Oracle and fetch data from USER_TAB_MODIFICATIONS
    oracle_hook = OracleHook(oracle_conn_id)
    oracle_conn = oracle_hook.get_conn()

    oracle_query = """
    SELECT TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME, INSERTS, UPDATES, DELETES, TIMESTAMP, TRUNCATED, DROP_SEGMENTS
    FROM USER_TAB_MODIFICATIONS
    """

    oracle_data = []
    with oracle_conn.cursor() as oracle_cursor:
        oracle_cursor.execute(oracle_query)
        oracle_data = oracle_cursor.fetchall()

    # Connect to PostgreSQL and insert data into the snapshot_all_tab_modifications table
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()

    postgres_insert_query = """
    INSERT INTO snapshot_all_tab_modifications (
        snapshot_dt, table_owner, table_name, partition_name, subpartition_name, inserts, updates, deletes, "timestamp", truncated, drop_segments)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    snapshot_dt = datetime.now()
    with postgres_conn.cursor() as postgres_cursor:
        for row in oracle_data:
            postgres_cursor.execute(postgres_insert_query, (snapshot_dt, schema_name,) + row)
        postgres_conn.commit()


def get_snapshot(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()

    compare_snapshots_query = """
        WITH snapshot_all_tab_modifications_ordered AS (
            SELECT 
                row_number() OVER (PARTITION BY table_owner, table_name, partition_name, subpartition_name ORDER BY snapshot_dt DESC) rn,
                *
            FROM snapshot_all_tab_modifications
        )
        SELECT 
            cur.table_owner,
            cur.table_name,
            cur.partition_name,
            cur.subpartition_name,
            cur.inserts,
            cur.updates,
            cur.deletes
        FROM snapshot_all_tab_modifications_ordered cur
        WHERE cur.rn = 1;
    """

    with postgres_conn.cursor() as cursor:
        cursor.execute(compare_snapshots_query)
        snapshot_results = cursor.fetchall()

    # Pass the comparison results to the next task using XCom
    kwargs["task_instance"].xcom_push("snapshot_results", snapshot_results)


def get_partition_label(table_name, high_value):
    partition_label = None

    if table_name in tables_partitioned_by_day:
        high_value_date = high_value - timedelta(days=1)
        partition_label = f"{table_name}_{high_value_date.strftime('%Y_%m_%d')}"
    elif table_name in tables_partitioned_by_month:
        high_value_date = high_value - timedelta(days=1)
        partition_label = f"{table_name}_{high_value_date.strftime('%Y_%m')}"

    return partition_label


def get_high_value(table_name, partition_name, subpartition_name):
    oracle_hook = OracleHook(oracle_conn_id)
    conn = oracle_hook.get_conn()
    
    if subpartition_name is not None:
        query = f"SELECT high_value FROM user_tab_subpartitions WHERE table_name = '{table_name}' AND subpartition_name = '{subpartition_name}'"
    else:
        query = f"SELECT high_value FROM user_tab_partitions WHERE table_name = '{table_name}' AND partition_name = '{partition_name}'"

    with conn.cursor() as cursor:
        cursor.execute(query)
        high_value = cursor.fetchone()[0]

    # Extract the date from the high_value string
    date_regex = r'\d{4}-\d{2}-\d{2}'
    date_string = re.search(date_regex, high_value).group(0)

    return date_string


def export_non_partitioned_table_to_parquet(table_name):
    oracle_hook = OracleHook(oracle_conn_id)
    conn = oracle_hook.get_conn()

    query = f"SELECT * FROM {table_name}"
    columns_query = f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table_name}' ORDER BY column_id"

    with conn.cursor() as cursor:
        cursor.execute(columns_query)
        columns = [row[0] for row in cursor.fetchall()]

        cursor.execute(query)
        data = cursor.fetchall()

    df = pd.DataFrame(data, columns=columns)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"output/parquet/{table_name}.parquet")


def export_partition_to_parquet(table_name, partition_name, partition_label):
    oracle_hook = OracleHook(oracle_conn_id)
    conn = oracle_hook.get_conn()

    query = f"SELECT * FROM {table_name} PARTITION ({partition_name})"
    columns_query = f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table_name}' ORDER BY column_id"

    with conn.cursor() as cursor:
        cursor.execute(columns_query)
        columns = [row[0] for row in cursor.fetchall()]

        cursor.execute(query)
        data = cursor.fetchall()

    df = pd.DataFrame(data, columns=columns)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"output/parquet/{table_name}_{partition_label}.parquet")


def export_changed_data(**kwargs):
    comparison_results = kwargs["task_instance"].xcom_pull(task_ids="get_snapshot", key="snapshot_results")

    if comparison_results is None:
        logging.error("No comparison results found.")
        return
    
    changed_tables = set()
    changed_partitions = set()

    for row in comparison_results:
        _, table_name, partition_name, subpartition_name, inserts_diff, updates_diff, deletes_diff = row

        if any([inserts_diff, updates_diff, deletes_diff]):
            if partition_name is None:
                changed_tables.add(table_name)
            else:
                changed_partitions.add((table_name, partition_name, subpartition_name))

    # Export changed tables from tables_non_partitioned
    for table_name in changed_tables:
        if table_name in tables_non_partitioned:
            export_non_partitioned_table_to_parquet(table_name)

    # Export changed partitions
    for table_name, partition_name, subpartition_name in changed_partitions:
        high_value = get_high_value(table_name, partition_name, subpartition_name)
        high_value_date = datetime.strptime(high_value, '%Y-%m-%d')
        partition_label = None

        if table_name in tables_partitioned_by_day:
            partition_label = high_value_date.strftime('%Y_%m_%d')
        elif table_name in tables_partitioned_by_month:
            partition_label = high_value_date.strftime('%Y_%m')

        if partition_label:
            export_partition_to_parquet(table_name, partition_name, partition_label)


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

create_snapshot_all_tab_modifications_task = PythonOperator(
    task_id="create_snapshot_all_tab_modifications",
    python_callable=create_snapshot_all_tab_modifications,
    provide_context=True,
    dag=dag,
)

get_snapshot_task = PythonOperator(
    task_id="get_snapshot",
    python_callable=get_snapshot,
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

flush_database_monitoring_info_task >> create_snapshot_all_tab_modifications_task >> get_snapshot_task >> export_changed_data_task ## >> upload_to_snowflake_task
