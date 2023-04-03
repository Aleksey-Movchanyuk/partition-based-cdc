import re
from datetime import timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.hooks.oracle_hook import OracleHook


def get_high_value(oracle_conn_id, table_name, partition_name):
    oracle_hook = OracleHook(oracle_conn_id)
    conn = oracle_hook.get_conn()
    
    query = f"SELECT high_value FROM user_tab_partitions WHERE table_name = '{table_name}' AND partition_name = '{partition_name}'"

    with conn.cursor() as cursor:
        cursor.execute(query)
        high_value = cursor.fetchone()[0]

    # Extract the date from the high_value string
    date_regex = r'\d{4}-\d{2}-\d{2}'
    date_string = re.search(date_regex, high_value).group(0)

    return date_string


def export_non_partitioned_table_to_parquet(oracle_conn_id, table_name):
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


def export_partition_to_parquet(oracle_conn_id, table_name, partition_name, partition_label):
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
