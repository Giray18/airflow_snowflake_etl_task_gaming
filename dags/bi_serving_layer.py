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


@aql.run_raw_sql
def left_join_table(table_1: Table, table_2: Table, table_3: Table, table_4: Table, table_5: Table):
    """left join operation get f_multi_ships fact table (left join of 3 tables to session started table)
    here we match all in session multiplayer battle, in_app_purchase, ship transaction in sessions for every
    user`s unique sessions"""
    return """
      CREATE OR REPLACE TABLE {{table_1}} as SELECT 
      * FROM {{table_2}} as m
      LEFT JOIN {{table_3}} as n  ON m.session_android_join_key = n.multiplayer_join_key
      LEFT JOIN {{table_4}} as b   ON m.session_android_join_key = b.in_app_join_key
      LEFT JOIN {{table_5}} as d  ON m.session_android_join_key = d.ship_trans_join_key;
    """


@aql.dataframe
def save_dataframe_to_snowflake(df: DataFrame,columns_to_rename):
# Renaming table columns as aligned with table name
    counter_rename = 0
    while counter_rename < len(columns_to_rename)-1:
        df.rename({columns_to_rename[counter_rename].lower() : columns_to_rename[counter_rename+1].lower()},inplace=True,axis='columns')
        counter_rename += 2
    return df


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
            columns_to_rename = programs.helpers()["golden_layer_col_map"][key.lower()]
            save_dataframe_bi = save_dataframe_to_snowflake(filtered_dataframes,columns_to_rename,output_table = Table(
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


# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()