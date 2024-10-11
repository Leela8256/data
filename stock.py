
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import timedelta, datetime
import pandas as pd
import requests
import snowflake.connector


def return_snowflake_conn():
    user_id = Variable.get('snowflake_user')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='dev',
        schema='raw_data'
    )
    return conn


default_args = {
    'owner': 'Leela',
    'depends_on_past': False,
    'email_on_failure': ['lionleela999@gmail.com'],
    'email_on_retry': ['lionleela999@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='stock',
    default_args=default_args,
    description='A DAG to fetch stock prices and insert into Snowflake',
    #schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 10),
    catchup=False,
    schedule = '00 23 * * *'
) as dag:

    @task()
    def extract(symbol):
        """Extract the last 90 days of stock prices."""
        vantage_api_key = Variable.get('vantage_api_key')
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
        r = requests.get(url)
        data = r.json()

        time_series = data.get("Time Series (Daily)", {})
        date_list = sorted(time_series.keys())[-90:]  # Last 90 days
        data_last_90_days = []
        for date in date_list:
            daily_data = time_series[date]
            data_last_90_days.append({
                "date": date,
                "open": float(daily_data["1. open"]),
                "high": float(daily_data["2. high"]),
                "low": float(daily_data["3. low"]),
                "close": float(daily_data["4. close"]),
                "volume": int(daily_data["5. volume"]),
                "symbol": symbol
            })

        df = pd.DataFrame(data_last_90_days)
        return df.to_dict(orient="records")  

    @task()
    def create_stock_data_table():
        """Create the stock prices table in Snowflake."""
        conn = return_snowflake_conn()
        curr = conn.cursor()

        create_table_query = """
        CREATE OR REPLACE TABLE raw_data.stock_prices (
            date DATE PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INTEGER,
            symbol VARCHAR
        );
        """
        try:
            curr.execute(create_table_query)
            print("Table created successfully.")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            curr.close()

    @task()
    def insert_stock_data(df_data):
        """Insert stock data into the Snowflake table ensuring idempotency."""
        conn = return_snowflake_conn()
        curr = conn.cursor()

        df = pd.DataFrame(df_data)
        insert_query = """
        INSERT INTO raw_data.stock_data (date, open, high, low, close, volume, symbol)
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM raw_data.stock_prices
            WHERE date = %s AND symbol = %s
        )
        """

        try:
            conn.cursor().execute("BEGIN")
            for index, row in df.iterrows():
                curr.execute(insert_query,
                             (row['date'], row['open'], row['high'], row['low'],
                              row['close'], row['volume'], row['symbol'],
                              row['date'], row['symbol']))
            conn.cursor().execute("COMMIT")
            print("Data inserted successfully.")
        except Exception as e:
            conn.cursor().execute("ROLLBACK")
            print(f"Error inserting data: {e}")
        finally:
            curr.close()

    @task()
    def check_table():
        """Fetch and print the first row to verify data insertion."""
        conn = return_snowflake_conn()
        curr = conn.cursor()
        try:
            curr.execute("SELECT * FROM raw_data.stock_prices LIMIT 1")
            first_row = curr.fetchone()
            print(f"First row in the table: {first_row}")
        except Exception as e:
            print(f"Error fetching data: {e}")
        finally:
            curr.close()


    symbol = "MSFT"  
    stock_data = extract(symbol)
    create_table = create_stock_data_table()
    insert_data = insert_stock_data(stock_data)
    check_stats = check_table()

    create_table >> stock_data >> insert_data >> check_stats
