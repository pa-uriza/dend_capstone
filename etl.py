import configparser
from datetime import datetime
import os
from typing import List

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, IntegerType


def SAS2date(date: int) -> datetime:
    """
    Transforms date in SAS format to a datetime object

    Args:
        date (int): Date in SAS format

    Returns:
        datetime: Datetime object
    """
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')


SAS2date_udf = udf(SAS2date, DateType())


def rename_columns(table, new_columns: List[str]):
    """
    Renames the columns of a given spark DataFrame

    Args:
        table (spark.DataFrame): Table to change columns from
        new_columns (List[String]): New names for table's columns

    Returns:
        spark.DataFrame: Table with new column names
    """
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def process_i94_data(spark: SparkSession, input_data: str,
                     output_data: str) -> None:
    """
    Processes i94 immigration data and gets the Dim_Traveler and Fact_Traveler
    tables for the dimensional model.

    Args:
        spark (SparkSession): Spark session object
        input_data (str): s3 bucket where data is read from
        output_data (str): s3 bucket where data is written to
    """
    label_file = os.path.join(input_data, "I94_SAS_Labels_Descriptions.SAS")
    with open(label_file) as f:
        labels = f.readlines()

    country_code = {}
    for countries in labels[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    country_df = spark.createDataFrame(country_code.items(),
                                       ['code', 'country'])

    filename = os.path.join(
        input_data, 'immigration/18-83510-I94-Data-2016/*.sas7bdat')
    df = pd.read_sas(filename, 'sas7bdat', encoding="ISO-8859-1")

    # fact_traveler table
    fact_traveler = df.select('cicid', 'i94addr', 'i94port', 'i94bir',
                              'count').distinct()

    fact_cols = ['cicid', 'state_code', 'iata_code', 'age', 'count']
    fact_traveler = rename_columns(fact_traveler, fact_cols)

    fact_traveler.write.mode("overwrite").partitionBy('state_code')\
        .parquet(path=output_data + 'fact_traveler')

    # dim_traveler table
    dim_traveler = df.select('cicid', 'arrdate', 'depdate', 'i94yr',
                             'i94mon', 'i94cit', 'i94res', 'i94visa',
                             'biryear', 'gender', 'visatype').distinct()

    dim_traveler = dim_traveler.withColumn(
        'i94cit',
        dim_traveler['i94cit'].cast(IntegerType())
    )
    dim_traveler = dim_traveler.withColumn(
        'i94res',
        dim_traveler['i94res'].cast(IntegerType())
    )

    dim_traveler = dim_traveler.join(
        country_df, on=dim_traveler.i94cit == country_df.code, how='left')

    dim_traveler = dim_traveler.withColumnRenamed('country', 'citizenship')

    dim_traveler = dim_traveler.join(
        country_df, on=dim_traveler.i94res == country_df.code, how="left")
    dim_traveler = dim_traveler.withColumnRenamed('country', 'residence')
    dim_traveler = dim_traveler.withColumnRenamed('i94visa', 'reason')

    dim_traveler = dim_traveler.select('cicid', 'arrdate', 'depdate', 'i94yr',
                                       'i94mon', 'citizenship', 'residence',
                                       'reason', 'biryear', 'gender',
                                       'visatype')

    dim_cols = ['cicid', 'arrdate', 'depdate', 'year', 'month', 'citizenship',
                'residence', 'reason', 'biryear', 'gender', 'visatype']
    dim_traveler = rename_columns(dim_traveler, dim_cols)

    dim_traveler.write.mode("overwrite")\
        .parquet(path=output_data + 'dim_traveler')


def process_demography_data(spark: SparkSession, input_data: str,
                            output_data: str) -> None:
    """
    Processes US demography data and gets the Dim_Demography table for the
    dimensional model.

    Args:
        spark (SparkSession): Spark session object
        input_data (str): s3 bucket where data is read from
        output_data (str): s3 bucket where data is written to
    """

    demog_data = os.path.join(
        input_data, 'demography/us-cities-demographics.csv')
    df = spark.read.format('csv').options(
        header=True, delimiter=';').load(demog_data)

    dim_demography = df.select(['State Code', 'State', 'Race', 'Median Age',
                                'Male Population', 'Female Population',
                                'Total Population']).distinct()

    dim_cols = ['state_code', 'state_name', 'race', 'median_age',
                'male_population', 'female_population', 'total_population']
    dim_demography = rename_columns(dim_demography, dim_cols)

    dim_demography.write.mode("overwrite")\
                        .parquet(path=output_data + 'dim_demography')


def process_airport_data(spark: SparkSession, input_data: str,
                         output_data: str) -> None:
    """
    Processes Airport data and gets the Dim_Airports table for the dimensional
    model.

    Args:
        spark (SparkSession): Spark session object
        input_data (str): s3 bucket where data is read from
        output_data (str): s3 bucket where data is written to
    """

    demog_data = os.path.join(
        input_data, 'airports/airport-codes_csv.csv')
    df = spark.read.format('csv').options(
        header=True, delimiter=',').load(demog_data)

    dim_airports = df.select(['iata_code', 'type', 'name', 'elevation_ft',
                              'continent', 'iso_country', 'iso_region',
                              'municipality', 'coordinates']).distinct()

    dim_airports.write.mode("overwrite")\
        .parquet(path=output_data + 'dim_airports')


def main():
    config = configparser.ConfigParser()
    config.read('config.cfg', encoding='utf-8-sig')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']
    ['AWS_SECRET_ACCESS_KEY']
    SOURCE_BUCKET = config['S3']['SOURCE_BUCKET']
    DESTINATION_BUCKET = config['S3']['DESTINATION_BUCKET']

    spark = SparkSession.builder\
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0")\
        .enableHiveSupport().getOrCreate()

    process_i94_data(spark, SOURCE_BUCKET, DESTINATION_BUCKET)
    process_demography_data(spark, SOURCE_BUCKET, DESTINATION_BUCKET)
    process_airport_data(spark, SOURCE_BUCKET, DESTINATION_BUCKET)


if __name__ == "__main__":
    main()
