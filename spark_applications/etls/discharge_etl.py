import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType

spark = SparkSession.builder.appName('discharge_etl').getOrCreate()
sc = spark.sparkContext

# Right justify a string column with length number of zeroes.
get_rjust_udf = lambda length: F.udf(lambda z: str(z).rjust(length, '0') if z else None, StringType())

spark.read.option("header", "true").csv("s3://udacity-data-engineering-nanodegree-capstone-project/data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2010.csv")\
    .withColumn("Total Costs", F.col("Total Costs").cast(DoubleType()))\
    .withColumn("Total Charges", F.col("Total Charges").cast(DoubleType()))\
    .withColumn("Length of Stay", F.col("Length of Stay").cast(IntegerType()))\
    .withColumn("APR DRG Code", get_rjust_udf(3)(F.col("APR DRG Code")))\
    .withColumn("APR MDC Code", get_rjust_udf(2)(F.col("APR MDC Code")))\
    .select("Facility ID", "Age Group", "Gender", "Race", "Length of Stay", "Discharge Year", "APR DRG Code", "APR MDC Code", "APR Severity of Illness Code", "APR Risk of Mortality", "Total Charges", "Total Costs")\
    .withColumnRenamed("Facility ID", "facility_id")\
    .withColumnRenamed("Age Group", "age_group")\
    .withColumnRenamed("Gender", "gender")\
    .withColumnRenamed("Race", "race")\
    .withColumnRenamed("Length of Stay", "days_of_stay")\
    .withColumnRenamed("Discharge Year", "discharge_year")\
    .withColumnRenamed("APR DRG Code", "apr_drg_code")\
    .withColumnRenamed("APR MDC Code", "apr_mdc_code")\
    .withColumnRenamed("APR Severity of Illness Code", "apr_severity_of_illness_code")\
    .withColumnRenamed("APR Risk of Mortality", "apr_risk_of_mortality")\
    .withColumnRenamed("Total Charges", "total_charges")\
    .withColumnRenamed("Total Costs", "total_costs")\
    .write.mode("overwrite").partitionBy("apr_severity_of_illness_code")\
    .parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/discharges.parquet")