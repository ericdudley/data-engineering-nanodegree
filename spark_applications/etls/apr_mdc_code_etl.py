from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('apr_mdc_code_etl').getOrCreate()
sc = spark.sparkContext

spark.read.option("header", "true").csv("s3://udacity-data-engineering-nanodegree-capstone-project/data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2010.csv")\
    .select("APR MDC Code", "APR MDC Description")\
    .withColumnRenamed("APR MDC Code", "code")\
    .withColumnRenamed("APR MDC Description", "description")\
    .distinct()\
    .write.mode("overwrite")\
    .parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/apr_mdc_code.parquet")