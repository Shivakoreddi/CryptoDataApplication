from apiWrapper import coinAPI
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey
import sqlite3
from sqlite3 import Error
import pandas as pd
import os

def extract(path):
    for filename in os.listdir(path):
        if filename.startswith('valid_category'):
            file = filename

    category_df = pd.read_csv(file, sep=',')
    return category_df

def upsert_load(df,conn):
    cursor = conn.cursor()
    query = []
    for index,row in df.iterrows():
        query.append((row['category_id'], row['name'],row['market_cap'], row['updated_at']))


    ##execute query
    cursor.executemany("insert or replace into category values(?,?,?,?)",query)
    cursor.execute("select * from category")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.commit()
    return


def main():
    path = "/CryptoDataApplication/"
    df = extract(path)
    ##create db connection
    conn = sqlite3.connect('/CryptoDataApplication/transactionDB/tradingSchema.db')
    ##generate upsert queries
    upsert_load(df,conn)


if __name__=="__main__":
    main()