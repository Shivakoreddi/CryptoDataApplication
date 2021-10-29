import requests
import json
from requests.adapters import HTTPAdapter

class coinAPI:
    def __init__(self,format="json"):
        self.format = format
        self.base_url = "https://api.coingecko.com/api/v3/"
        self.request_timeout = 120
        self.session = requests.Session()

    def __request(self,url):
        try:
            response = self.session.get(url,timeout = self.request_timeout)
        except requests.exceptions.RequestException:
            raise

        try:
            response.raise_for_status()
            content = json.loads(response.content.decode('utf-8'))
            return content
        except Exception as e:
            content = json.loads(response.content.decode('utf-8'))
            raise ValueError(content)
    def ping(self):
        api_url = '{}ping'.format(self.base_url)
        return self.__request(api_url)
    def base_url(self):
        return self.__request(self.base_url)

    def get_coins_list(self,**kwargs):
        """list of coins with data (name,price,market,developer,community)"""
        api_url = '{}coins/list'.format(self.base_url)

        return self.__request(api_url)

    def get_coins_market(self,vs_currency):
        """list all market data includes coins price,market cap,volume """
        """sample url : https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"""
        api_url = '{0}coins/markets?vs_currency={1}'.format(self.base_url,vs_currency)
        return self.__request(api_url)

    def get_coin_categories_list(self):
        """list all categories"""
        api_url = '{}coins/categories/list'.format(self.base_url)
        return self.__request(api_url)

    def get_categories_market(self):
        """list all categories with market data"""
        api_url = '{}coins/categories'.format(self.base_url)
        return self.__request(api_url)

    def get_exchanges_list(self):
        """list all exchanges"""
        api_url = '{}exchanges'.format(self.base_url)
        return self.__request(api_url)

    def get_exchanges_name_list(self):
        """list all supported markets id and name """
        api_url = '{}exchanges/list'.format(self.base_url)
        return self.__request(api_url)

    def get_derivatives(self):
        """list all derivatives tickers"""
        api_url = '{}derivatives'.format(self.base_url)
        return self.__request(api_url)

    def get_derivatives_exchanges(self):
        """list all derivatives tickers """
        api_url = '{}derivatives/exchanges'.format(self.base_url)
        return self.__request(api_url)

    def get_derivatives_exchanges_list(self):
        """list all derivatives tickers"""
        api_url = '{}derivatives/exchanges/list'.format(self.base_url)
        return self.__request(api_url)


