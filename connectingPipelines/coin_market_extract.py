from apiWrapper import coinAPI
from datetime import datetime
from connectingPipelines import schema_validation as svt
import logging
import pandas as pd
import boto3

ACCESS_KEY = 'AY5WVnnnnnnnnnnn2'
SECRET_KEY = '6Wnnnnnnnnnnn'

def coinsMarket(cmkt):
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    for cm in cmkt:

        df1 = df1.append({
            'id':cm['id'],
            'symbol':cm['symbol'],
            'name':cm['name'],
            'image':cm['image']
        },ignore_index=True)


        df2 = df2.append({
            'market_id':cm['id'],
            'market_symbol':cm['symbol'],
            'market_name':cm['name'],
            'image':cm['image'],
            'current_price':cm['current_price'],
            'market_cap':cm['market_cap'],
            'market_cap_rank':cm['market_cap_rank'],
            'total_volume':cm['total_volume'],
            'high_24h':cm['high_24h'],
            'low_24h':cm['low_24h'],
            'price_change_24h':cm['price_change_24h'],
            'price_change_percent_24h':cm['price_change_percentage_24h'],
            'market_cap_change_24h':cm['market_cap_change_24h'],
            'market_cap_change_percent_24h':cm['market_cap_change_percentage_24h'],
            'circulating_supply':cm['circulating_supply'],
            'total_supply':cm['total_supply'],
            'max_supply':cm['max_supply'],
            'ath':cm['ath'],
            'ath_change_percent':cm['ath_change_percentage'],
            'ath_date':cm['ath_date'],
            'atl':cm['atl'],
            'atl_change_percent':cm['atl_change_percentage'],
            'atl_date':cm['atl_date'],
            'last_updated':cm['last_updated']

        },ignore_index=True)
    ##write coins dataframe into json file
    coin_filename = "coin_" + str(datetime.now().strftime("%Y_%m_%d"))
    coin_filename = coin_filename+".csv"
    df1.to_csv(("{}".format(coin_filename)))
    ##data = pd.read_csv(coin_filename)
    coin_schema = svt._validator('coins')
    coin_errors = coin_schema.validate(df1,columns=coin_schema.get_column_names())
    errors_index_rows = [e.row for e in coin_errors]
    coin_clean = df1.drop(index=errors_index_rows)
    coin_clean.to_csv(("valid_{}".format(coin_filename)))
    coinfilename = "valid_{}"+coin_filename
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    s3.upload_file(coinfilename, 'files', coinfilename)


    ##market data file
    mkt_filename = "market_" + str(datetime.now().strftime("%Y_%m_%d"))
    mkt_filename = mkt_filename + ".csv"
    ##write market dataframe into json file
    df2.to_csv(("{}".format(mkt_filename)))

    #market data validation
    try:
        mkt_schema = svt._validator('market')
        mkt_errors = mkt_schema.validate(df2, columns=mkt_schema.get_column_names())
        msg = mkt_errors
        ##print(msg[0])
        mkt_errors_index_rows = [e.row for e in mkt_errors]
        ##print(mkt_errors_index_rows)
        mkt_clean = df2.drop(index=mkt_errors_index_rows,axis=1)
        mkt_clean.to_csv(("valid_{}".format(mkt_filename)))
        marketfilename = "valid_{}" + mkt_filename
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3.upload_file(marketfilename, 'files', marketfilename)
    except Exception as e:
        print(e)
        ##0x09427A30
    return

def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='coin_market_extract.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    cg = coinAPI()
    ##Extract category data
    cmkt = cg.get_coins_market('usd')
    log.info("files are extracted!!")
    #store into json file
    coinsMarket(cmkt)
    log.info("coins.json and market.json files created in staging!!")



if __name__=="__main__":
    main()