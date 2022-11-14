import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('data_quality_etl').getOrCreate()
sc = spark.sparkContext

def verify_apr_drg_code():
    """
    Perform data quality checks on the apr_drg_code table.
    """

    df = spark.read.parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/apr_drg_code.parquet")\
        .withColumn("code_lengths", F.length("code"))\
        .withColumn("mdc_code_lengths", F.length("mdc_code"))

    # There are exactly DRG codes, each one should be represented in this table.
    assert df.count() == 334

    # All DRG codes should have a length between 1 and 3
    code_lengths_df = df.select(F.min(F.col('code_lengths')).alias("min_code_length"), F.max(F.col('code_lengths')).alias('max_code_length')).first()
    assert code_lengths_df.min_code_length == code_lengths_df.max_code_length == 3

    # All MDC codes should have a length of 2
    code_lengths_df = df.select(F.min(F.col('mdc_code_lengths')).alias("min_code_length"), F.max(F.col('mdc_code_lengths')).alias('max_code_length')).first()
    assert code_lengths_df.min_code_length == code_lengths_df.max_code_length == 2

def verify_apr_mdc_code():
    """
    Perform data quality checks on the apr_mdc_code table.
    """
    df = spark.read.parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/apr_mdc_code.parquet")\
        .withColumn("code_lengths", F.length("code"))

    # There are exactly 26 MDC codes, each one should be represented in this table.
    assert df.count() == 26

    # All MDC codes should have a length of 2
    code_lengths_df = df.select(F.min(F.col('code_lengths')).alias("min_code_length"), F.max(F.col('code_lengths')).alias('max_code_length')).first()
    assert code_lengths_df.min_code_length == code_lengths_df.max_code_length == 2

verify_apr_drg_code()
verify_apr_mdc_code()

