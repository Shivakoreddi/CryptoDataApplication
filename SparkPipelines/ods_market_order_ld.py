from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lower, lit, upper, initcap, length, from_unixtime, substring, length, expr, \
    current_date
##redditor = df.drop('_c0')
import getpass
import logging

username = getpass.getuser()


def main(spark):
    logging.basicConfig(level=logging.CRITICAL,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    """Main ETL definition
    :return :None
    """

    data = extract_task(spark)
    data_transformed = transformation_task(data)
    load_data(data_transformed)

    # log the success and terminate spark application
    # log.warn('Job is Finished')
    spark.stop()
    return None


def extract_task(spark):
    """Load data from csv file format.
    #param spark: Spark session object.
    #return: Spark Dataframe.
    """
    market_df = spark.read.csv('/user/xxxx/userCryptoStorage/valid_market*', header=True)

    return market_df


def transformation_task(df):
    """Tranform original dataset.

    :param_df: Input Dataframe.
    :return: Transformed DataFrame.
    """
    ##Prepare dataframes
    spark.sql('use xxx_usercrypto_db')
    ##create staging table
    # spark.sql('drop table if exists stg_users')
    # df.write.saveAsTable('stg_users')
    market_df = df.drop('_c0')
    mainDF = spark.sql('select * from ods_market_order')
    delta = market_df.withColumn('last_updated', current_date()).select(col('market_id').alias('mo_id'),
                                                                        col('market_symbol').alias('mo_symbol'),
                                                                        col('market_name').alias('mo_name'),
                                                                        col('current_price').alias('mo_price'),
                                                                        col('market_cap'),
                                                                        col('market_cap_rank').alias('market_rank'),
                                                                        col('circulating_supply').alias(
                                                                            'current_supply'),
                                                                        col('total_supply'), col('last_updated'))

    ##joining dataframes
    main = mainDF.alias('main')
    delta = delta.alias('delta')
    updatedDF = main. \
        join(delta, main.mo_id == delta.mo_id, 'outer')
    upsertDF = updatedDF.where((~col("main.mo_id").isNull()) & (~col("delta.last_updated").isNull())).select(
        "delta.*").distinct()
    unchangedDF = updatedDF.where(col("main.mo_id").isNull()).select("delta.*")
    ##delta= redditor_df.withColumn('updated_date',lit(None).cast('string'))
    unchangedDF = unchangedDF.withColumn('last_updated', lit(None).cast('string'))
    finalDF = upsertDF.union(unchangedDF)
    return finalDF


def load_data(finalDF):
    """write to table.

    :param df: DataFrame to print.
    :return: None
    """
    finalDF.createOrReplaceTempView('temp_finaldf')
    spark.sql('''insert OVERWRITE TABLE ods_market_order SELECT * FROM  temp_finaldf''')
    s = spark.sql('select count(*) from ods_market_order')
    print(s)
    return None


# entry point for Pyspark ETL Application
if __name__ == '__main__':
    # Start Spark Application and Spark Session,logger and config
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        enableHiveSupport(). \
        appName(f'{username} | cda_ods_market_order'). \
        master('yarn'). \
        getOrCreate()

    main(spark)
