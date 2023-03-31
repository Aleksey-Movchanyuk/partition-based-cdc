# Oracle -> Snowflake Data Synchronization POC

This proof of concept (POC) demonstrates how to migrate changed partitions from an Oracle database to a Snowflake database using Apache Airflow, Parquet files, and Python.

## Overview

The POC consists of an Apache Airflow DAG that performs the following steps:

1. Gather statistics for an Oracle table.
2. Find the partitions changed since the last DAG run.
3. Export changed partitions into Parquet files.
4. Upload Parquet files to Snowflake.

## Requirements

- Python 3.6 or higher
- Apache Airflow
- Oracle database with a partitioned table
- Snowflake account with a target database and schema
- Python libraries: `cx_Oracle`, `pandas`, `pyarrow`, and `snowflake-connector-python`

You can install the required Python libraries using the following command:

```bash
pip install cx_Oracle pandas pyarrow snowflake-connector-python
```

### Setup

1. Install Apache Airflow following the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html).
2. Place the oracle_to_snowflake_dag.py file in your Airflow DAGs folder.
3. Replace the placeholders in oracle_to_snowflake_dag.py with your Oracle and Snowflake connection details, schema names, and table names.
4. Start the Airflow web server and scheduler.
5. Access the Airflow web interface and enable the oracle_to_snowflake DAG.


### Usage

Once the DAG is enabled in Airflow, it will run according to the specified schedule (daily, by default). The DAG will gather statistics for the specified Oracle table, find the changed partitions since the last run, export the changed partitions to Parquet files, and then upload the Parquet files to Snowflake.

You can also trigger the DAG manually from the Airflow web interface or using the Airflow CLI:

```
airflow dags trigger oracle_to_snowflake
```

### Notes

* Ensure that you have the necessary privileges to access the Oracle and Snowflake databases, gather statistics, and perform data migration operations.
* This POC assumes that the target Snowflake table has the same structure as the source Oracle table. You may need to modify the code if your target table has a different structure or additional columns for metadata.
* You can customize the DAG schedule, retry settings, and other parameters according to your needs.

This `README.md` provides an overview of the POC, requirements, setup instructions, and usage information. You may customize the content as needed for your specific use case.