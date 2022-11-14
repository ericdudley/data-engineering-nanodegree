import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('discharge_data_quality').getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/discharges.parquet")

# Check that charges and costs are within a reasonable range of values.
total_charges_df = df.select(F.min(F.col('total_charges')).alias("min_total_charges"), F.max(F.col('total_charges')).alias('max_total_charges')).first()

assert total_charges_df.min_total_charges >= 0 and total_charges_df.max_total_charges <= 15000000

total_costs_df = df.select(F.min(F.col('total_costs')).alias("min_total_costs"), F.max(F.col('total_costs')).alias('max_total_costs')).first()

assert total_costs_df.min_total_costs >= 0 and total_costs_df.max_total_costs <= 15000000

codes_df = df.withColumn("code_lengths", F.length("apr_drg_code"))\
        .withColumn("mdc_code_lengths", F.length("apr_mdc_code"))

# All DRG codes should have a length between 1 and 3
code_lengths_df = codes_df.select(F.min(F.col('code_lengths')).alias("min_code_length"), F.max(F.col('code_lengths')).alias('max_code_length')).first()
assert code_lengths_df.min_code_length == code_lengths_df.max_code_length == 3

# All MDC codes should have a length of 2
code_lengths_df = codes_df.select(F.min(F.col('mdc_code_lengths')).alias("min_code_length"), F.max(F.col('mdc_code_lengths')).alias('max_code_length')).first()
assert code_lengths_df.min_code_length == code_lengths_df.max_code_length == 2