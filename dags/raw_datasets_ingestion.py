from datetime import datetime
from airflow import Dataset
from airflow.models import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pandas import DataFrame
import pandas as pd
import programs
from faker import Faker
from faker.providers import DynamicProvider

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
    return df


# Basic DAG definition
dag = DAG(
    dag_id="raw_tables_ingestion",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    # Create sample dataframes by using PROGRAMS class and methods created
    # Sample dataframes saved into snowflake database w_gaming_task

    table_names_list = programs.helpers()["table_names_list"]

    for key,value in table_names_list.items():
            # Creating dataframes by create_dataframe function
            vars() [key] = programs.create_dataframe(value)
            save_data = transform_dataframe(vars() [key],output_table = Table(
            name = f"{key}_raw_dataframe",
            conn_id = SNOWFLAKE_CONN_ID,))

# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()