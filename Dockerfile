FROM quay.io/astronomer/astro-runtime:10.2.0

RUN pip install apache-airflow-providers-microsoft-mssql 

RUN pip install apache-airflow-providers-common-sql

RUN pip install apache-airflow-providers-odbc

RUN pip install snowflake-sqlalchemy-json

RUN pip install faker

RUN pip install apache-airflow-providers-snowflake
