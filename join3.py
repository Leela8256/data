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
@task
def create_session_summary():
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            CREATE OR REPLACE TABLE dev.analytics.session_summary AS
                  WITH ranked_sessions AS (
                            SELECT
                            u.userid,
                            u.sessionid,
                            u.channel,
                            s.ts,
                 ROW_NUMBER() OVER (PARTITION BY u.sessionid ORDER BY s.ts) AS rn
            FROM
                dev.raw_data.user_session_channel u
            JOIN
                dev.raw_data.session_timestamp s
            ON
                 u.sessionid = s.sessionid
            WHERE
                    u.userid IS NOT NULL
             AND     s.ts IS NOT NULL
                        )
                SELECT
                    userid,
                    sessionid,
                    channel,
                    ts
                FROM
                    ranked_sessions
                    WHERE
                    rn = 1;
                    """)
    finally:
        cur.close()
        conn.close()
with DAG(
    dag_id='join3',
    start_date=datetime(2024, 10, 20),
    catchup=False,
    schedule_interval='00 6 * * *',
    tags=['ELT'],
) as dag:
    create_session_summary_task = create_session_summary()

   