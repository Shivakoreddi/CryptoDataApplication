
from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey,Float
import sqlite3
from sqlite3 import Error
import pandas as pd
import logging

SQLITE = 'sqlite'

#table name
COINS = 'coins'
MARKET = 'market'
EXCHANGE_TYPE = 'exchange_type'
DERIVATIVES = 'derivatives'
EXCHANGE = 'exchange'
COIN_EXCHANGE = 'coin_exchange'
COIN_CATEGORY = 'coin_category'
CATEGORY = 'category'


class TradingDB:
    DB_ENGINE = {
        SQLITE :'sqlite://{DB}'
    }

    db_engine = None
    def __init__(self,username='',password='',dbname=''):

        engine_url = 'sqlite:///{DB}'.format(DB=dbname)
        self.db_engine = create_engine(engine_url)
        logging.info(self.db_engine)



    def create_db_tables(self):
        metadata = MetaData()
        coins = Table(COINS, metadata,
                      Column('id',String,primary_key=True),
                      Column('symbol',String),
                      Column('name',String),
                      Column('image',String)
                      )
        market = Table(MARKET,metadata,
                       Column('market_id',String,primary_key=True),
                       Column('market_symbol',String),
                       Column('market_name',String),
                       Column('image',String),
                       Column('current_price',Float),
                       Column('market_cap',Float),
                       Column('market_cap_rank',Integer),
                       Column('total_volume',Float),
                       Column('high_24h',Float),
                       Column('low_24h',Float),
                       Column('price_change_24h',Float),
                       Column('price_change_percent_24h',Float),
                       Column('market_cap_change_24h',Float),
                       Column('market_cap_change_percent_24h',Float),
                       Column('circulating_supply',Float),
                       Column('total_supply',Float),
                       Column('max_supply',Float),
                       Column('ath',Float),
                       Column('ath_change_percent',Float),
                       Column('ath_date',String),
                       Column('atl',Float),
                       Column('atl_change_percent',Float),
                       Column('atl_date',String),
                       Column('last_updated',String))
        coin_category = Table(COIN_CATEGORY,metadata,
                              Column('coin_category_id',String,primary_key=True),
                              Column('Category_id',String),
                              Column('Coin_id',String),
                              Column('active',Integer)
                              )
        exchange_type = Table(EXCHANGE_TYPE,metadata,
                              Column('Exch_type_id',Integer,primary_key=True),
                              Column('Exch_type_name',String),
                              Column('Exch_type_value',String))
        derivatives = Table(DERIVATIVES,metadata,
                            Column('id',String,primary_key=True),
                            Column('name',String),
                            Column('image',String),
                            Column('open_interest_btc',String),
                            Column('trade_volume_24h_btc',String),
                            Column('year_established',String),
                            Column('country',String),
                            Column('url',String),
                            Column('exchange_type',String))
        exchange = Table(EXCHANGE,metadata,
                         Column('id',String,primary_key=True),
                         Column('name',String),
                         Column('year_established',String),
                         Column('country',String),
                         Column('url',String),
                         Column('image',String),
                         Column('trust_score',Integer),
                         Column('trust_score_rank',Integer),
                         Column('trade_volume_24h_btc',Float),
                         Column('exchange_type',String))
        category = Table(CATEGORY,metadata,
                         Column('category_id',String,primary_key=True),
                         Column('name',String),
                         Column('market_cap',Float),
                         Column('updated_at',String)
                         )
        coin_exchange = Table(COIN_EXCHANGE,metadata,
                              Column('coin_exchange_id',Integer,primary_key=True),
                              Column('exchange_id',String),
                              Column('coin_id',String)
                              )
        try:
            metadata.create_all(self.db_engine)
            logging.info("Tables Created")
        except Exception as e:
            logging.error("Error occured during Table creation")

    def execute_query(self,query=''):
        if query =='': return
        ##print(query)
        with self.db_engine.connect() as connection:
            try:
                connection.execute(query)
            except Exception as e:
                logging.error(e)

def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='schemaCreation.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Schema Creation")
    dbname = 'tradingSchema.db'
    dbms = TradingDB(SQLITE,dbname=dbname)
    dbms.create_db_tables()

if __name__=="__main__":
    main()