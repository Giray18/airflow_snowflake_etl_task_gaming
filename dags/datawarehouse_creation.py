from datetime import datetime
from airflow import Dataset
from airflow.operators.dummy import DummyOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pandas import DataFrame
import pandas as pd
import programs

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Import SQLAlchemy to set constraints on some temporary tables
import sqlalchemy

# Define constants/variables for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_CONN_ID_2 = "snowflake_dwh_task"
table_names_list_dict = {"IN_APP_PURCHASE_DF" : "IN_APP_PURCHASE_DF_RAW_DATAFRAME", "LOGIN_DF" : "LOGIN_DF_RAW_DATAFRAME",
                         "MULTIPLAYER_BATTLE_DF" : "MULTIPLAYER_BATTLE_DF_RAW_DATAFRAME",
                        "NEW_USER_DF" : "NEW_USER_DF_RAW_DATAFRAME" ,"SESSION_STARTED_DF" :"SESSION_STARTED_DF_RAW_DATAFRAME",
                        "SHIP_TRANSACTION_DF" : "SHIP_TRANSACTION_DF_RAW_DATAFRAME"}


@aql.run_raw_sql
def create_table(table: Table):
    """Create user_id table on Snowflake"""
    return """
      CREATE OR REPLACE TABLE {{table}} as SELECT 
      user_id, user_geo_location, user_is_spender, join_key FROM 
      session_started_df_dwh;
    """

@aql.run_raw_sql
def anti_join_table(table_1: Table, table_2: Table):
    """Anti join operation get only not new users to user_id table"""
    return """
      CREATE OR REPLACE TABLE {{table_1}} as SELECT 
      * FROM {{table_1}} u
      WHERE NOT EXISTS (
      SELECT user_id FROM 
      {{table_2}} n WHERE u.user_id = n.user_id) ;
    """


@aql.dataframe
def save_dataframe_to_snowflake(df: DataFrame):
    return df


@aql.dataframe
def transform_dataframe(df: DataFrame):
    # Dropping USER_GEO_LOCATION column
    df = df.drop(columns=['user_geo_location'])
    # Deduplication on user_id table df by user_id number
    df = df.drop_duplicates(subset=['user_id'])
    return df

@aql.transform
#Reading only needed columns from raw data tables
def filter_source_table(input_table: Table,my_string):
    return "SELECT "+ f"{my_string}"+ " FROM {{input_table}}"


# Basic DAG definition
dag = DAG(
    dag_id="read_raw_data_tables",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:



    # Create a Table objects for table operations on snowflake
    with TaskGroup('bulk_operations') as tg1:
        for key,value in table_names_list_dict.items():
            # Getting tables from raw_data layer into variables to used in further operations
            vars() [key] = Table(
            name = value,
            conn_id=SNOWFLAKE_CONN_ID,)
            # String to get columns names from list to string to pass in select statement
            my_string = ",".join(str(element) for element in programs.helpers()["silver_layer_col_names"][key.lower()])
            filtered_dataframes = filter_source_table(vars() [key],my_string)
            # Saving tables into dwh_layer on related snowflake schema
            save_dataframe_dwh = save_dataframe_to_snowflake((filtered_dataframes),output_table = Table(
            name = f"{key}_dwh",
            conn_id = SNOWFLAKE_CONN_ID_2,))

    # Saving tables from snowflake to variables to use on tasks for tables that has further transformations
    user_id_df_dwh = Table(name="user_id_df_dwh", temp=True, conn_id=SNOWFLAKE_CONN_ID_2)
    new_user_df_dwh = Table(name="new_user_df_dwh", temp=True, conn_id=SNOWFLAKE_CONN_ID_2)

    # Task dependencies
    tg1 >> create_table(table=user_id_df_dwh, conn_id=SNOWFLAKE_CONN_ID_2) >> anti_join_table(table_1 = user_id_df_dwh, table_2 = new_user_df_dwh) >> transform_dataframe(user_id_df_dwh)



# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()