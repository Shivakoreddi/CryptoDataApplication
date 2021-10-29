from apiWrapper import coinAPI
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey
import sqlite3
from sqlite3 import Error
import pandas as pd
import os
"""create table coin_exchange as select c.coin_id||'_'||c.exchange_id as coin_exchange_id,
        c.exchange_id,c.coin_id from (select id as coin_id,(select id from exchange order by RANDOM() limit 1) as exchange_id
         from coins) c"""
def coin_exch_execute(conn):
    cursor = conn.cursor()
    cursor.execute("create table if not exists coin_exchange (coin_exchange_id text,exchange_id text,coin_id text)")
    cursor.execute("select * from coin_exchange")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    try:
        ##for i in range(100):
            ##cursor.execute(
                """insert into coin_exchange select c.coin_id||'_'||c.exchange_id as coin_exchange_id,
        c.exchange_id,c.coin_id from (select id as coin_id,(select name from exchange order by RANDOM() limit 1) as exchange_id
         from coins order by random() limit 1) c"""
        ##conn.commit()
    except Exception as e:
        print(e)
        return False
    return True

def main():
    path = "/CryptoDataApplication/"
    ##create db connection
    conn = sqlite3.connect('/CryptoDataApplication/transactionDB/tradingSchema.db')
    if coin_exch_execute(conn):
        return True
    else:
        return False


if __name__=="__main__":
    main()