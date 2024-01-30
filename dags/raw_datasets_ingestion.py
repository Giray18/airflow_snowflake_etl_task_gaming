from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pandas import DataFrame
import pandas as pd
import programs
# from faker import Faker
# from faker.providers import DynamicProvider

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"

# Define a function for transforming tables to dataframes
@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Creating Unique value holding JOIN_KEY Column for JOIN operations on further stages
    df["JOIN_KEY"] = df["SESSION_ID"].astype(str) + df["USER_ID"].astype(str)
    return df


# Basic DAG definition
dag = DAG(
    dag_id="raw_tables_ingestion",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:



    # Create a Table object for customer data in the Snowflake database
    with TaskGroup('bulk_write_operations') as tg1:
        # Create sample dataframes by using (importing) PROGRAMS class and methods manually created
        # Sample dataframes saved into snowflake database w_gaming_task public schema

        # Gets df/table names from helpers method and pass to a loop for bulk operation
        table_names_list_raw = programs.helpers()['table_names_list']

        # Creating snowflake ingestion schema`s table
        for key,value in table_names_list_raw.items():
                # Creating dataframes by create_dataframe function and saving into a variable
                vars() [key] = programs.create_dataframe(value)
                # Saving created dataframes into snowflake`s raw_layer (ingestion layer)`
                save_data = transform_dataframe(vars() [key],output_table = Table(
                name = f"{key}_raw_dataframe",
                conn_id = SNOWFLAKE_CONN_ID,))

    trigger_dependent_dag = TriggerDagRunOperator(
    task_id="trigger_dependent_dag",
    trigger_dag_id="read_raw_data_tables",
    wait_for_completion=True,
    deferrable=False,  
    )

    # Task dependencies
    tg1 >> trigger_dependent_dag

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()