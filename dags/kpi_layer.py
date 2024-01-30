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
# SNOWFLAKE_CONN_ID_4 = "snowflake_kpi_task"
table_names_list_dict = {"f_multi_ships" : "F_MULTI_SHIPS_BI", "d_new_user" : "d_new_user_bi",
                         "d_user_id" : "d_user_id_bi"}
                        # "d_new_user" : "NEW_USER_DF_DWH" ,"d_session_started" :"SESSION_STARTED_DF_DWH",
                        # "d_ship_transaction" : "SHIP_TRANSACTION_DF_DWH", "d_user_id" : "USER_ID_DF_DWH"}

# Active users count
@aql.run_raw_sql
def active_users_daily(table_1: Table):
    """active user count from f_multi_ships table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW active_users_daily as SELECT 
      COUNT(DISTINCT session_user_id) AS active_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2
      ;
    """

@aql.run_raw_sql
def active_users_weekly(table_1: Table):
    """active user count from f_multi_ships table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW active_users_weekly as SELECT 
      COUNT(DISTINCT session_user_id) AS active_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8
      ;
    """

@aql.run_raw_sql
def active_users_monthly(table_1: Table):
    """active user count from f_multi_ships table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW active_users_monthly as SELECT 
      COUNT(DISTINCT session_user_id) AS active_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31
      ;
    """

# New Users count
@aql.run_raw_sql
def new_users_daily(table_1: Table, table_2: Table):
    """new user count from f_multi_ships table with inner join new_user table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW new_users_daily as SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2
      ;
    """

@aql.run_raw_sql
def new_users_weekly(table_1: Table, table_2: Table):
    """new user count from f_multi_ships table with inner join new_user table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW new_users_weekly as SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8
      ;
    """

@aql.run_raw_sql
def new_users_monthly(table_1: Table, table_2: Table):
    """new user count from f_multi_ships table with inner join new_user table (unique count of session_user_id column)"""
    return """
      CREATE OR REPLACE VIEW new_users_monthly as SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31
      ;
    """

# Revenue calculations
@aql.run_raw_sql
def revenue_daily(table_1: Table):
    """revenue calculation by f_multi_ships table (sum of in_app_usd_cost)"""
    return """
      CREATE OR REPLACE VIEW revenue_daily as SELECT 
      SUM(in_app_usd_cost) AS revenue_daily FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2
      ;
    """

@aql.run_raw_sql
def revenue_weekly(table_1: Table):
    """revenue calculation by f_multi_ships table (sum of in_app_usd_cost)"""
    return """
      CREATE OR REPLACE VIEW revenue_weekly as SELECT 
      SUM(in_app_usd_cost) AS revenue_weekly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8
      ;
    """

@aql.run_raw_sql
def revenue_monthly(table_1: Table):
    """revenue calculation by f_multi_ships table (sum of in_app_usd_cost)"""
    return """
      CREATE OR REPLACE VIEW revenue_monthly as SELECT 
      SUM(in_app_usd_cost) AS revenue_monthly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31
      ;
    """

# Spenders(Buyers) calculations
@aql.run_raw_sql
def spenders_daily(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW spenders_daily as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_daily FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2 AND
      session_user_is_spender = TRUE
      ;
    """

@aql.run_raw_sql
def spenders_weekly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW spenders_weekly as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_weekly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8 AND
      session_user_is_spender = TRUE
      ;
    """

@aql.run_raw_sql
def spenders_monthly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW spenders_monthly as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_weekly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31 AND
      session_user_is_spender = TRUE
      ;
    """

# ARPU Calculations (Revenue/Active User Count)
@aql.run_raw_sql
def ARPU_daily(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARPU_daily as SELECT 
      COUNT(DISTINCT session_user_id) AS active_users_daily,
      SUM(in_app_usd_cost) AS revenue_daily,
      cast(round(revenue_daily / active_users_daily,2) as numeric(36,2)) AS ARPU_daily FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2 
      ;
    """

@aql.run_raw_sql
def ARPU_weekly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARPU_weekly as SELECT 
      COUNT(DISTINCT session_user_id) AS active_users_weekly,
      SUM(in_app_usd_cost) AS revenue_weekly,
      cast(round(revenue_weekly / active_users_weekly,2) as numeric(36,2)) AS ARPU_weekly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8 
      ;
    """

@aql.run_raw_sql
def ARPU_monthly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARPU_monthly as SELECT 
      COUNT(DISTINCT session_user_id) AS active_users_monthly,
      SUM(in_app_usd_cost) AS revenue_monthly,
      cast(round(revenue_monthly / active_users_monthly,2) as numeric(36,2)) AS ARPU_monthly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31 
      ;
    """

# ARRPU Calculations (Revenue/Spender User Count)
@aql.run_raw_sql
def ARRPU_daily(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARRPU_daily as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_daily,
      SUM(in_app_usd_cost) AS revenue_daily,
      cast(round(revenue_daily / spenders_daily,2) as numeric(36,2)) AS ARRPU_daily FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2 AND
      session_user_is_spender = TRUE
      ;
    """


@aql.run_raw_sql
def ARRPU_weekly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARRPU_weekly as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_weekly,
      SUM(in_app_usd_cost) AS revenue_weekly,
      cast(round(revenue_weekly / spenders_weekly,2) as numeric(36,2)) AS ARRPU_weekly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8 AND
      session_user_is_spender = TRUE
      ;
    """

@aql.run_raw_sql
def ARRPU_monthly(table_1: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW ARRPU_monthly as SELECT 
      COUNT(DISTINCT session_user_id) AS spenders_monthly,
      SUM(in_app_usd_cost) AS revenue_monthly,
      cast(round(revenue_monthly / spenders_monthly,2) as numeric(36,2)) AS ARRPU_monthly FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 31 AND
      session_user_is_spender = TRUE
      ;
    """

# 1 Day Retention Rate (Rate of New Users who played multi game in last 1 day)
@aql.run_raw_sql
def one_day_retention_rate(table_1: Table, table_2: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW one_day_retention_rate as 
      WITH NEW_USER_COUNT (new_user_count) AS (SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2)
      SELECT new_user_count,
      (SELECT COUNT(DISTINCT multiplayer_user_id) AS multi_played_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 2) AS multiplayer_user_count,
      cast(round(multiplayer_user_count / new_user_count,2) as numeric(36,2)) AS one_day_retention_rate
      FROM NEW_USER_COUNT
      ;
    """
# 3 Day Retention Rate (Rate of New Users who played multi game in last 3 day)
@aql.run_raw_sql
def three_day_retention_rate(table_1: Table, table_2: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW three_day_retention_rate as 
      WITH NEW_USER_COUNT (new_user_count) AS (SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 4)
      SELECT new_user_count,
      (SELECT COUNT(DISTINCT multiplayer_user_id) AS multi_played_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 4) AS multiplayer_user_count,
      cast(round(multiplayer_user_count / new_user_count,2) as numeric(36,2)) AS three_day_retention_rate
      FROM NEW_USER_COUNT
      ;
    """

# 7 Day Retention Rate (Rate of New Users who played multi game in last 7 day)
@aql.run_raw_sql
def seven_day_retention_rate(table_1: Table, table_2: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW seven_day_retention_rate as 
      WITH NEW_USER_COUNT (new_user_count) AS (SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8)
      SELECT new_user_count,
      (SELECT COUNT(DISTINCT multiplayer_user_id) AS multi_played_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8) AS multiplayer_user_count,
      cast(round(multiplayer_user_count / new_user_count,2) as numeric(36,2)) AS seven_day_retention_rate
      FROM NEW_USER_COUNT
      ;
    """

# 7 Day Conversion Rate (Rate of New Users who played multi game in last 7 day)
@aql.run_raw_sql
def seven_day_conversion_rate(table_1: Table, table_2: Table):
    """spender user count by f_multi_ships table (distinct count of session_user_id filtered by related column)"""
    return """ 
      CREATE OR REPLACE VIEW seven_day_conversion_rate as 
      WITH NEW_USER_COUNT (new_user_count) AS (SELECT 
      COUNT(DISTINCT session_user_id) AS new_user_count FROM {{table_1}} f INNER JOIN {{table_2}} d 
      ON f.session_user_id = d.user_user_id
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8)
      SELECT new_user_count,
      (SELECT COUNT(DISTINCT in_app_user_id) AS item_purchased_user_count FROM {{table_1}}
      WHERE DATEDIFF(day,session_event_timestamp,GETDATE()) < 8) AS item_purchased_user_count,
      cast(round(item_purchased_user_count / new_user_count,2) as numeric(36,2)) AS seven_day_conversion_rate
      FROM NEW_USER_COUNT
      ;
    """

# Ships owned by a every user every day (At this stage SC_AMOUNT > 0 Used since I do not have 0 value for that column in my sample dataset)
@aql.run_raw_sql
def user_ship_table(table_1: Table):
    """A table showing daily based ship-user_id intersections"""
    return """ 
      CREATE OR REPLACE VIEW user_ship_table as 
      SELECT session_user_id, ship_trans_ship_name,session_event_timestamp,COUNT(*) AS ship_quantity
      FROM {{table_1}} 
      WHERE ship_trans_ship_name is not null
      GROUP BY session_user_id,session_event_timestamp,ship_trans_ship_name
      ;
    """
#  SC_AMOUNT < 0 == ship purchased by user

# Daily ships popularity
@aql.run_raw_sql
def daily_ship_table(table_1: Table):
    """daily purchasements by users"""
    return """ 
      CREATE OR REPLACE VIEW daily_ship_table as 
      SELECT session_user_id, ship_trans_ship_name,session_event_timestamp,COUNT(*) AS ship_quantity
      FROM {{table_1}} 
      WHERE ship_trans_ship_name is not null AND SHIP_TRANS_SC_AMOUNT < 0
      GROUP BY session_user_id,session_event_timestamp,ship_trans_ship_name
      ;
    """





@aql.transform
#Reading only needed columns from raw data tables
def filter_source_table(input_table: Table,my_string):
    return "SELECT "+ f"{my_string}"+ " FROM {{input_table}}"


# Basic DAG definition
dag = DAG(
    dag_id="kpi_calculations",
    start_date=datetime(2024, 1, 12),
    schedule="@daily",
    catchup=False,
)

with dag:
    with TaskGroup('kpi_group_1') as tg1:
        # Saving tables from snowflake to variables to use on KPI calculations
        f_multi_ships = Table(name="f_multi_ships_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
        d_new_user = Table(name="d_new_user_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)
        d_user_id = Table(name="d_user_id_bi", temp=True, conn_id=SNOWFLAKE_CONN_ID_3)

        # Active Users Calculation
        active_users_daily(f_multi_ships)
        active_users_weekly(f_multi_ships)
        active_users_monthly(f_multi_ships)

        # New Users Calculation
        new_users_daily(f_multi_ships,d_new_user)
        new_users_weekly(f_multi_ships,d_new_user)
        new_users_monthly(f_multi_ships,d_new_user)

        # Revenue Calculations
        revenue_daily(f_multi_ships)
        revenue_weekly(f_multi_ships)
        revenue_monthly(f_multi_ships)

        # Spender (Users) Calculations
        spenders_daily(f_multi_ships)
        spenders_weekly(f_multi_ships)
        spenders_monthly(f_multi_ships)

        # ARPU Calculations
        ARPU_daily(f_multi_ships)
        ARPU_weekly(f_multi_ships)
        ARPU_monthly(f_multi_ships)

        # ARRPU Calculations
        ARRPU_daily(f_multi_ships)
        ARRPU_weekly(f_multi_ships)
        ARRPU_monthly(f_multi_ships)

        # Retention Rate Calculations
        one_day_retention_rate(f_multi_ships,d_new_user)
        three_day_retention_rate(f_multi_ships,d_new_user)
        seven_day_retention_rate(f_multi_ships,d_new_user)

        # Convertion Rate Calculations
        seven_day_conversion_rate(f_multi_ships,d_new_user)

    with TaskGroup('kpi_group_2') as tg1:
        # Ships owned by a every user every day
        user_ship_table(f_multi_ships)
        daily_ship_table(f_multi_ships)



    # Create a Table objects for table operations on snowflake
    # with TaskGroup('bulk_operations') as tg1:
    #     for key,value in table_names_list_dict.items():
    #         # Getting tables from raw_data layer into variables to used in further operations
    #         vars() [key] = Table(
    #         name = value,
    #         conn_id=SNOWFLAKE_CONN_ID_3,)
            # String to get columns names from list to string to pass in select statement
            # my_string = ",".join(str(element) for element in programs.helpers()["golden_layer_col_names"][key.lower()])
            # filtered_dataframes = filter_source_table(vars() [key],my_string.lower())
            # # Saving tables into bi_layer on related snowflake schema
            # columns_to_rename = programs.helpers()["golden_layer_col_map"][key.lower()]
            # save_dataframe_bi = save_dataframe_to_snowflake(filtered_dataframes,columns_to_rename,output_table = Table(
            # name = f"{key}_kpi",
            # conn_id = SNOWFLAKE_CONN_ID_3,))



    # Task dependencies
    # tg1 >> left_join_table(table_1 = f_multi_ships_bi, table_2 = d_session_started_bi , table_3 = d_multiplayer_battle_bi 
    #                        , table_4 = d_in_app_purchase_bi, table_5 = d_ship_transaction_bi) 


# Delete temporary and unnamed tables created by `load_file` and `transform`, in this example
    aql.cleanup()