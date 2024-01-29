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
SNOWFLAKE_CONN_ID_3 = "snowflake_bi_task"
SNOWFLAKE_CONN_ID_2 = "snowflake_dwh_task"
table_names_list_dict = {"d_in_app_purchase" : "IN_APP_PURCHASE_DF_DWH", "d_login" : "LOGIN_DF_DWH",
                         "d_multiplayer_battle" : "MULTIPLAYER_BATTLE_DF_DWH",
                        "d_new_user" : "NEW_USER_DF_DWH" ,"d_session_started" :"SESSION_STARTED_DF_DWH",
                        "d_ship_transaction" : "SHIP_TRANSACTION_DF_DWH", "d_user_id" : "USER_ID_DF_DWH"}


# @aql.run_raw_sql
# def create_table(table: Table):
#     """Create user_id table on Snowflake"""
#     return """
#       CREATE OR REPLACE TABLE {{table}} as SELECT 
#       user_id, user_geo_location, user_is_spender, join_key FROM 
#       session_started_df_dwh;
#     """

@aql.run_raw_sql
def left_join_table(table_1: Table, table_2: Table, table_3: Table, table_4: Table, table_5: Table):
    """left join operation get f_multi_ships fact table (left join of 3 tables to session started table)
    here we match all in session multiplayer battle, in_app_purchase, ship transaction in sessions for every
    user`s unique sessions"""
    return """
      CREATE OR REPLACE TABLE {{table_1}} as SELECT 
      * FROM {{table_2}} m
      LEFT JOIN {{table_3}} n  ON m.join_key = n.join_key
      LEFT JOIN {{table_4}} b   ON m.join_key = b.join_key
      LEFT JOIN {{table_5}} d  ON m.join_key = d.join_key;
    """


@aql.dataframe
def save_dataframe_to_snowflake(df: DataFrame):
    # df.rename(columns={"A": "a", "B": "c"})
    return df


# @aql.dataframe
# def transform_dataframe(df: DataFrame):
#     # Deduplication on user_id table df by user_id number
#     df = df.drop_duplicates(subset=['user_id'])
#     return df

@aql.transform
#Reading only needed columns from raw data tables
def filter_source_table(input_table: Table,my_string):
    return "SELECT "+ f"{my_string}"+ " FROM {{input_table}}"


# Basic DAG definition
dag = DAG(
    dag_id="read_dwh_layer_data_tables",
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
            conn_id=SNOWFLAKE_CONN_ID_2,)
            # String to get columns names from list to string to pass in select statement
            my_string = ",".join(str(element) for element in programs.helpers()["golden_layer_col_names"][key.lower()])
            filtered_dataframes = filter_source_table(vars() [key],my_string.lower())
            # Saving tables into bi_layer on related snowflake schema
            save_dataframe_bi = save_dataframe_to_snowflake((filtered_dataframes),output_table = Table(
            name = f"{key}_bi",
            conn_id = SNOWFLAKE_CONN_ID_3,))

    # Saving tables from snowflake to variables to use on tasks for tables that has further transformations
    f_multi_ships_bi = Table(name="f_multi_ships_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
    d_multiplayer_battle_bi = Table(name="d_multiplayer_battle_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
    d_session_started_bi = Table(name="d_session_started_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
    d_in_app_purchase_bi = Table(name="d_in_app_purchase_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
    d_ship_transaction_bi = Table(name="d_ship_transaction_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)

    # Task dependencies
    tg1 >> left_join_table(table_1 = f_multi_ships_bi, table_2 = d_session_started_bi , table_3 = d_multiplayer_battle_bi 
                           , table_4 = d_in_app_purchase_bi, table_5 = d_ship_transaction_bi) 
    # >> anti_join_table(table_1 = user_id_df_dwh, table_2 = new_user_df_dwh) >> transform_dataframe(user_id_df_dwh)



# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()