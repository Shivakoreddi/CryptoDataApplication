# CryptoDataApplication
Crypto API gateway —this is our main source of data, we are collecting coins, markets, exchanges, coin categories, derivatives etc.
Aws Lambda (market Lambda) — This function is scheduled to fetch latest market data and store into S3 staging area. The zipped lambda function is available in our repository.
Initial File Staging (S3) — In this partition, we are I am storing all the initial fetch mentioned earlier like coins, markets etc. and also schema validated
OLTP System — We have two transaction schemas (i) Trading schema — this is our main schema were our source data is recorded on daily basis. (ii) User Portfolio Schema — this schema is to service our webservice ,were user will login and maintain coins portfolio (we are assigning 5 coins in their portfolio for every new user account, this coins market data is refreshed through pipelines from s3 to portfolio schema)
User portfolio service — this is simple webservice developed on flask, were user can login and maintain portfolio of coins.
 ODS Staging (S3) — This is another S3 partitioned area were data is recorded in csv form, these data is fetched from both the above schemas for our later usage.
ODS Area — this is implemented on Spark sql warehouse database, hence there are certain limitations like unsupported update/delete process(so we have implemented this through dividing data frame as update and insert from outer joins), 

![image](https://user-images.githubusercontent.com/42261408/139371796-4f860e6a-5607-4ac5-8fa2-021ee1b5f0f1.png)
