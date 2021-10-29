
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation,IsDtypeValidation,MatchesPatternValidation
from pandas_schema.validation_warning import ValidationWarning
from decimal import *
import numpy as np





def check_float(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True

def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

def _validator(schema_type:str):
    #validations
    string_validation = [CustomElementValidation(lambda s:str(s),'is not string')]
    float_validation = [CustomElementValidation(lambda f:check_float(f),'is not float')]
    int_validation = [CustomElementValidation(lambda i:check_int(i),'is not integer')]
    null_validation = [CustomElementValidation(lambda n:n is not np.nan,'field is cannot be null')]

    if schema_type == 'coins':
        coins_schema = pandas_schema.Schema([
            Column('id',null_validation),
            Column('symbol'),
            Column('name'),
            Column('image')])
        return coins_schema

    elif schema_type=='market':
        market_schema = pandas_schema.Schema([
            Column('market_id',null_validation),
            Column('market_symbol'),
            Column('market_name'),
            Column('image'),
            Column('current_price',int_validation),
            Column('market_cap',int_validation),
            Column('market_cap_rank',int_validation),
            Column('total_volume',int_validation),
            Column('high_24h',int_validation),
            Column('low_24h',int_validation),
            Column('price_change_24h'),
            Column('price_change_percent_24h'),
            Column('market_cap_change_24h'),
            Column('market_cap_change_percent_24h'),
            Column('circulating_supply'),
            Column('total_supply'),
            Column('max_supply'),
            Column('ath'),
            Column('ath_change_percent'),
            Column('ath_date'),
            Column('atl'),
            Column('atl_change_percent'),
            Column('atl_date'),
            Column('last_updated')
        ])
        return market_schema

    elif schema_type=='derivatives':
        derv_schema = pandas_schema.Schema([
            Column('id',null_validation),
            Column('name'),
            Column('open_interest_btc'),
            Column('trade_volume_24h_btc'),
            Column('year_established'),
            Column('country'),
            Column('url'),
            Column('exchange_type')
            ])
        return derv_schema

    elif schema_type=='exchanges':
        exch_schema = pandas_schema.Schema([
            Column('id',null_validation),
            Column('name'),
            Column('year_established'),
            Column('country'),
            Column('url'),
            Column('image'),
            Column('trust_score',int_validation),
            Column('trust_score_rank',int_validation),
            Column('trade_volume_24h_btc'),
            Column('exchange_type')
        ])
        return exch_schema

    elif schema_type=='category':
        cat_schema = pandas_schema.Schema([
            Column('category_id',null_validation),
            Column('name'),
            Column('market_cap'),
            Column('updated_at')
        ])
        return cat_schema
    else:
        return False


