
import sqlite3
from sqlite3 import Error
import pandas as pd
import os

def extract(path):
    for filename in os.listdir(path):
        if filename.startswith('valid_market'):
            file = filename

    market_df = pd.read_csv(file, sep=',')
    return market_df

def upsert_load(df,conn):
    cursor = conn.cursor()
    query = []
    ##params = [(market_id,market_symbol,market_name,image,current_price,
    ##    market_cap,market_cap_rank,total_volume,high_24h,low_24h,price_change_24h,price_change_percent_24h,market_cap_change_24h,
    ##    market_cap_change_percent_24h,circulating_supply,total_supply,max_supply,ath,ath_change_percent,ath_date,atl,atl_change_percent,atl_date,
    ##    last_updated)]
    for index,row in df.iterrows():
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
    ##cursor.execute("select * from market")
    ##rows = cursor.fetchall()
    ##for row in rows:
        ##print(row)
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