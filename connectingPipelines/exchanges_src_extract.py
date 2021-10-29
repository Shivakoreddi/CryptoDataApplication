from apiWrapper import coinAPI
from connectingPipelines import schema_validation as svt
from datetime import datetime
import logging
import pandas as pd
import boto3

ACCESS_KEY = 'yyyyyyyyyyyyyyyy'
SECRET_KEY = 'tttrJrrrrrP'

def exchange_data(exchange):
    df = pd.DataFrame()
    for exch in exchange:
        df = df.append({
            'id':exch['id'],
            'name':exch['name'],
            'year_established':exch['year_established'],
            'country':exch['country'],
            'url':exch['url'],
            'image':exch['image'],
            'trust_score':exch['trust_score'],
            'trust_score_rank':exch['trust_score_rank'],
            'trade_volume_24h_btc':exch['trade_volume_24h_btc'],
            'exchange_type':'spot'
        },ignore_index=True)
    ##write coins dataframe into json file
    exch_filename = "exchange_" + str(datetime.now().strftime("%Y_%m_%d"))
    exch_filename = exch_filename+".csv"
    ##write coins dataframe into json file
    df.to_csv(("{}".format(exch_filename)))
    ##data = pd.read_csv(coin_filename)
    try:
        exch_schema = svt._validator('exchanges')
        exch_errors = exch_schema.validate(df, columns=exch_schema.get_column_names())
        errors_index_rows = [e.row for e in exch_errors]
        exch_clean = df.drop(index=errors_index_rows)
        exch_clean.to_csv(("valid_{}".format(exch_filename)))
        exchfilename = "valid_{}" + exch_filename
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3.upload_file(exchfilename, 'files', exchfilename)
    except Exception as e:
        print(e)
    return

def category_data(category):
    df = pd.DataFrame()
    for cat in category:
        df = df.append({
            'category_id':cat['id'],
            'name':cat['name'],
            'market_cap':cat['market_cap'],
            'updated_at':cat['updated_at']
        },ignore_index=True)
    ##write category dataframe into json file
    cat_filename = "category_" + str(datetime.now().strftime("%Y_%m_%d"))
    cat_filename = cat_filename+".csv"

    ##write category dataframe into json file
    df.to_csv(("{}".format(cat_filename)))
    ##data = pd.read_csv(category_filename)
    try:
        cat_schema = svt._validator('category')
        cat_errors = cat_schema.validate(df, columns=cat_schema.get_column_names())
        errors_index_rows = [e.row for e in cat_errors]
        cat_clean = df.drop(index=errors_index_rows)
        cat_clean.to_csv(("valid_{}".format(cat_filename)))
        catfilename = "valid_{}" + cat_filename
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3.upload_file(catfilename, 'exchange-type-files', catfilename)
    except Exception as e:
        print(e)
    return

def derivatives_data(derivatives):
    df = pd.DataFrame()
    for derv in derivatives:
        df = df.append({
            'id':derv['id'],
            'name':derv['name'],
            'open_interest_btc':derv['open_interest_btc'],
            'trade_volume_24h_btc':derv['trade_volume_24h_btc'],
            'year_established':derv['year_established'],
            'country':derv['country'],
            'url':derv['url'],
            'exchange_type':'derivatives'
        },ignore_index=True)
    ##write derivatives dataframe into json file
    derv_filename = "derivatives_" + str(datetime.now().strftime("%Y_%m_%d"))
    derv_filename = derv_filename+".csv"
    ##write derivatives dataframe into json file
    df.to_csv(("{}".format(derv_filename)))
    ##data = pd.read_csv(derivatives_filename)
    try:
        derv_schema = svt._validator('derivatives')
        derv_errors = derv_schema.validate(df, columns=derv_schema.get_column_names())
        errors_index_rows = [e.row for e in derv_errors]
        derv_clean = df.drop(index=errors_index_rows)
        derv_clean.to_csv(("valid_{}".format(derv_filename)))
        dervfilename = "valid_{}" + derv_filename
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3.upload_file(dervfilename, 'files', dervfilename)
    except Exception as e:
        print(e)
    return



def main():
    log = logging.getLogger("app")
    logging.basicConfig(level=logging.DEBUG, filename='exchanges_src_extract.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    log.info("Starting Application")
    cg = coinAPI()
    ##Extract exchange_data
    exchange = cg.get_exchanges_list()
    log.info("exchange file extracted")
    #store into json file
    exchange_data(exchange)
    log.info("exchange.json file created!")
    ##Extract category data
    category = cg.get_categories_market()
    log.info("category file extracted")
    category_data(category)
    log.info("category.json file created")

    ##Extract derivatives data
    derivatives = cg.get_derivatives_exchanges()
    log.info("derivatives file extracted")
    derivatives_data(derivatives)
    log.info("derivatives.json file created")


if __name__=="__main__":
    main()
