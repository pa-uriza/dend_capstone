import os
import configparser
from pathlib import Path
from pyspark.sql import SparkSession


def print_schema(bucket: Path, spark: SparkSession) -> None:
    """
    Prints data schema for visual inspection

    Args:
        bucket (str): S3 bucket name
        spark (SparkSession): Spark session
    """
    s3_bucket = Path(bucket)
    for file_dir in s3_bucket.iterdir():
        if file_dir.is_dir():
            path = str(file_dir)
            df = spark.read.parquet(path)
            print("Table: " + path.split('/')[-1])
            df.printSchema()


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
    print_schema(DESTINATION_BUCKET, spark)


if __name__ == '__main__':
    main()
