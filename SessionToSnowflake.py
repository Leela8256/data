from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Function to get a Snowflake connection
def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn

# Task to create two tables in Snowflake
@task
def create_table(target_table1, target_table2):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table1} (
                userId int NOT NULL,
                sessionId varchar(32) PRIMARY KEY,
                channel varchar(32) DEFAULT 'direct'
            );
        """)
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table2} (
                sessionId varchar(32) PRIMARY KEY,
                ts timestamp
            );
        """)
    finally:
        cur.close()
        conn.close()

# Task to create or replace a stage in Snowflake
@task
def set_stage(stage_name):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """)
    finally:
        cur.close()
        conn.close()

# Task to load data from stage into Snowflake tables
@task
def load(stage_name, target_table1, target_table2):
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(f"""
            COPY INTO {target_table1} 
            FROM @{stage_name}/user_session_channel.csv;
        """)
        
        cur.execute(f"""
            COPY INTO {target_table2}
            FROM @{stage_name}/session_timestamp.csv;
        """)
    finally:
        cur.close()
        conn.close()

# Define the DAG
with DAG(
    dag_id='SessionToSnowflake',
    start_date=datetime(2024, 10, 20),
    catchup=False,
    schedule_interval='00 6 * * *',
    tags=['ETL'],
) as dag:
    tgt_table1 = "dev.raw_data.user_session_channel"
    tgt_table2 = "dev.raw_data.session_timestamp"
    stage = "dev.raw_data.blob_stage"

    # Define task dependencies
    create_tables_task = create_table(tgt_table1, tgt_table2)
    set_stage_task = set_stage(stage)
    load_data_task = load(stage, tgt_table1, tgt_table2)

    # Set task dependencies
    create_tables_task >> set_stage_task >> load_data_task
