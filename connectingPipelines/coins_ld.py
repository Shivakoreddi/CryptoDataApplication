from apiWrapper import coinAPI
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey
import sqlite3
from sqlite3 import Error
import pandas as pd
import os


def main():
    path = "/CryptoDataApplication/"
    for filename in os.listdir(path):
        if filename.startswith('valid_coin'):
            file = filename

    coin_df = pd.read_csv(file,sep=',')
    conn = sqlite3.connect('/CryptoDataApplication/transactionDB/tradingSchema.db')
    cursor = conn.cursor()
    query = []
    ##for index,row in coin_df.iterrows():
        ##query = """INSERT OR REPLACE INTO coins(id,symbol,name,image) VALUES('{0}','{1}','{2}','{3}')""".format(row['id'],row['symbol'],row['name'],row['image'])
        #print(query[1])
        ##cursor.execute(query)
        ##conn.commit()

    cursor.execute("select * from coins")
    rows = cursor.fetchall()
    for row in rows:
        print(row)


if __name__=="__main__":
    main()

