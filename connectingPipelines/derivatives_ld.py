import sqlite3
from sqlite3 import Error
import pandas as pd
import os

def extract(path):
    for filename in os.listdir(path):
        if filename.startswith('valid_derivative'):
            file = filename

    derivative_df = pd.read_csv(file, sep=',')
    return derivative_df

def upsert_load(df,conn):
    cursor = conn.cursor()
    query = []
    for index,row in df.iterrows():
        query.append((row['id'], row['name'],'null' ,row['open_interest_btc'], row['trade_volume_24h_btc'], row['year_established']
                                                           , row['country'], row['url'],row['exchange_type']))


    ##execute query
    cursor.executemany("insert or replace into derivatives values(?,?,?,?,?,?,?,?,?)",query)
    cursor.execute("select * from derivatives")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    ##conn.commit()
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