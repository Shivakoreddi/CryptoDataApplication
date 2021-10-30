from datetime import timedelta, datetime

from airflow import DAG
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

from airflow.utils.dates import days_ago

from airflow.hooks import SqliteHook
from airflow.operators import SqliteOperator

import sys
import sqlite3


sys.path.append('~cda_env/lib/python3.7/site-packages/')

import market_lambda_load



default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "start_date": datetime(2021, 10, 28),

    "retires": 0



}



dag = DAG(

    dag_id="market_lambda_DAG",

    default_args=default_args,

    catchup=False,

    schedule_interval="@once"

)

def upsert_load():

    market_df = pd.read_csv('market_daily.csv', sep=',')

    ##sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_trading_id')

    ##cursor = sqlite_hook.get_conn()

    conn = sqlite3.connect('/home/ec2-user/cda_env/lib/python3.7/site-packages/tradingSchema.db')

    cursor = conn.cursor()

    query = []

    ##params = [(market_id,market_symbol,market_name,image,current_price,

    ##    market_cap,market_cap_rank,total_volume,high_24h,low_24h,price_change_24h,price_change_percent_24h,market_cap_change_24h,

    ##    market_cap_change_percent_24h,circulating_supply,total_supply,max_supply,ath,ath_change_percent,ath_date,atl,atl_change_percent,atl_date,

    ##    last_updated)]

    for index,row in market_df.iterrows():

        query.append((row['market_id'], row['market_symbol'], row['market_name'],

                                                           row['image'], row['current_price'], row['market_cap']

                                                           , row['market_cap_rank'], row['total_volume'],

                                                           row['high_24h'], row['low_24h'], row['price_change_24h'],

                                                           row['price_change_percent_24h']

                                                           , row['market_cap_change_24h'],

                                                           row['market_cap_change_percent_24h'],

                                                           row['circulating_supply'], row['total_supply']

                                                           , row['max_supply'], row['ath'], row['ath_change_percent'],

                                                           row['ath_date'], row['atl'], row['atl_change_percent']

                                                           , row['atl_date'], row['last_updated']))





    ##print(query[1])

    ##execute query

    cursor.executemany("insert or replace into market values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",query)

    ##cursor.execute("select * from exchange")

    ##rows = cursor.fetchall()

    ##for row in rows:

        ##print(row)

    ##sqlite_hook.run(insert_cmd,parameters=row)

    conn.commit()

    return


start = DummyOperator(

    task_id="start",

    dag=dag

)



market_lambda_load = PythonOperator(

    task_id="market_lambda_load",

    dag=dag,

    python_callable=market_lambda_load.main

)

upsert_load = PythonOperator(
        task_id = "upsert_load",
        dag=dag,
        python_callable=upsert_load)


start>>market_lambda_load>>upsert_load
