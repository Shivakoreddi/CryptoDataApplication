
import sqlite3
from sqlite3 import Error
import pandas as pd
import os

def extract(path):
    for filename in os.listdir(path):
        if filename.startswith('valid_exchange'):
            file = filename

    exchange_df = pd.read_csv(file, sep=',')
    return exchange_df

def upsert_load(df,conn):
    cursor = conn.cursor()
    query = []
    ##params = [(market_id,market_symbol,market_name,image,current_price,
    ##    market_cap,market_cap_rank,total_volume,high_24h,low_24h,price_change_24h,price_change_percent_24h,market_cap_change_24h,
    ##    market_cap_change_percent_24h,circulating_supply,total_supply,max_supply,ath,ath_change_percent,ath_date,atl,atl_change_percent,atl_date,
    ##    last_updated)]
    for index,row in df.iterrows():
        query.append((row['id'], row['name'], row['year_established'],
                                                           row['country'], row['url'], row['image']
                                                           , row['trust_score'], row['trust_score_rank'],
                                                           row['trade_volume_24h_btc'], row['exchange_type']))


    ##print(query[1])
    ##execute query
    cursor.executemany("insert or replace into exchange values(?,?,?,?,?,?,?,?,?,?)",query)
    cursor.execute("select * from exchange")
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