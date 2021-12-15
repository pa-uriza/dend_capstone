import os
import configparser
from pathlib import Path
from pyspark.sql import SparkSession


def check_fact_traveler_schema(bucket: Path, spark: SparkSession) -> None:
    """
    Asserts Fact_Travelers table schema is valid

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    schema_str = ("StructType(List(StructField(cic_id,StringType,true),"
                  "StructField(iata_code,StringType,true),"
                  "StructField(age,StringType,true),"
                  "StructField(count,StringType,true),"
                  "StructField(state_code,StringType,true)))")
    path = os.path.join(bucket, 'fact_traveler')
    df = spark.read.parquet(path)
    assert str(df.schema) == schema_str, "Table schema is incorrect"


def check_dim_traveler_schema(bucket: Path, spark: SparkSession) -> None:
    """
    Asserts Dim_Travelers table schema is valid

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    schema_str = ("StructType(List(StructField(cicid,StringType,true),"
                  "StructField(arrdate,StringType,true),"
                  "StructField(depdate,StringType,true),"
                  "StructField(year,StringType,true),"
                  "StructField(month,StringType,true),"
                  "StructField(citizenship,StringType,true),"
                  "StructField(residence,StringType,true),"
                  "StructField(reason,StringType,true),"
                  "StructField(biryear,StringType,true),"
                  "StructField(gender,StringType,true),"
                  "StructField(visatype,StringType,true)))")
    path = os.path.join(bucket, 'dim_traveler')
    df = spark.read.parquet(path)
    assert str(df.schema) == schema_str, "Table schema is incorrect"


def check_dim_demography_schema(bucket: Path, spark: SparkSession) -> None:
    """
    Asserts Dim_Demography table schema is valid

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    schema_str = ("StructType(List(StructField(state_code,StringType,true),"
                  "StructField(state_name,StringType,true),"
                  "StructField(race,StringType,true),"
                  "StructField(median_age,StringType,true),"
                  "StructField(male_population,StringType,true),"
                  "StructField(female_population,StringType,true),"
                  "StructField(total_population,StringType,true)))")
    path = os.path.join(bucket, 'dim_demography')
    df = spark.read.parquet(path)
    assert str(df.schema) == schema_str, "Table schema is incorrect"


def check_dim_airports_schema(bucket: Path, spark: SparkSession) -> None:
    """
    Asserts Dim_Airports table schema is valid

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    schema_str = ("StructType(List(StructField(iata_code, StringType, true), "
                  "StructField(type, StringType, true), "
                  "StructField(name, StringType, true), "
                  "StructField(elevation_ft, StringType, true), "
                  "StructField(continent, StringType, true), "
                  "StructField(iso_country, StringType, true), "
                  "StructField(iso_region, StringType, true), "
                  "StructField(municipality, StringType, true), "
                  "StructField(coordinates, StringType, true)))")
    path = os.path.join(bucket, 'dim_airports')
    df = spark.read.parquet(path)
    assert str(df.schema) == schema_str, "Table schema is incorrect"


def check_non_empty_tables(bucket: str, spark: SparkSession) -> None:
    """
    Asserts that created tables are not empty

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    s3_bucket = Path(bucket)
    for file_dir in s3_bucket.iterdir():
        if file_dir.is_dir():
            path = str(file_dir)
            df = spark.read.parquet(path)
            record_num = df.count()
            assert record_num >= 0, ("Table: " + path.split('/')[-1] +
                                     " is empty!")


def main():
    config = configparser.ConfigParser()
    config.read('config.cfg', encoding='utf-8-sig')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']
    ['AWS_SECRET_ACCESS_KEY']
    DESTINATION_BUCKET = config['S3']['DESTINATION_BUCKET']

    spark = SparkSession.builder\
                        .config("spark.jars.packages",
                                "org.apache.hadoop:hadoop-aws:2.7.0")\
                        .enableHiveSupport().getOrCreate()

    check_non_empty_tables(DESTINATION_BUCKET, spark)
    check_fact_traveler_schema(DESTINATION_BUCKET, spark)
    check_dim_traveler_schema(DESTINATION_BUCKET, spark)
    check_dim_demography_schema(DESTINATION_BUCKET, spark)
    check_dim_airports_schema(DESTINATION_BUCKET, spark)


if __name__ == '__main__':
    main()
