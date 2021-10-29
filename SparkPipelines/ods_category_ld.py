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
    """Load data from parquet file format.
    #param spark: Spark session object.
    #return: Spark Dataframe.
    """
    cat_df = spark.read.csv('/user/xxx/userCryptoStorage/valid_category*', header=True)

    return cat_df


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
    cat_df = df.drop('_c0')
    mainDF = spark.sql('select * from ods_category')
    delta = cat_df.withColumn('updated_date', current_date()).select(col('category_id'),
                                                                     col('name').alias('category_name'),
                                                                     col('market_cap'), col('updated_date'))

    ##joining dataframes
    main = mainDF.alias('main')
    delta = delta.alias('delta')
    updatedDF = main. \
        join(delta, main.category_id == delta.category_id, 'outer')
    upsertDF = updatedDF.where((~col("main.category_id").isNull()) & (~col("delta.updated_date").isNull())).select(
        "delta.*").distinct()
    unchangedDF = updatedDF.where(col("main.category_id").isNull()).select("delta.*")
    ##delta= redditor_df.withColumn('updated_date',lit(None).cast('string'))
    unchangedDF = unchangedDF.withColumn('updated_date', lit(None).cast('string'))
    finalDF = upsertDF.union(unchangedDF)
    return finalDF


def load_data(finalDF):
    """write to table.

    :param df: DataFrame to print.
    :return: None
    """
    finalDF.createOrReplaceTempView('temp_finaldf')
    spark.sql('''insert OVERWRITE TABLE ods_category SELECT * FROM  temp_finaldf''')
    s = spark.sql('select count(*) from ods_category')
    print(s)
    return None


# entry point for Pyspark ETL Application
if __name__ == '__main__':
    # Start Spark Application and Spark Session,logger and config
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        enableHiveSupport(). \
        appName(f'{username} | cda_ods_category'). \
        master('yarn'). \
        getOrCreate()

    main(spark)
