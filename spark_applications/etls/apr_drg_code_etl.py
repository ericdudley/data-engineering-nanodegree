import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder.appName('apr_drg_code_etl').getOrCreate()
sc = spark.sparkContext

def to_array(row):
    """
    Convert a row containing a struct of (idx: str, value: str) to an array
    of values sorted by idx.
    """
    row_dict = row.asDict()
    row_values = list(row_dict.items())

    def key_func(val):
        return int(val[0])

    row_values.sort(key=key_func)
    return [val[1] for val in row_values]
    
to_array_udf = F.udf(lambda z: to_array(z), ArrayType(StringType()))

def rjust(vals):
    """
    Convert a row containing a list of strings / numbers into a list
    of strings containing integers right justified with zeroes to all
    be the same length.
    """
    str_vals = [str(int(val)) if val else '' for val in vals]
    max_length = 0
    for val in str_vals:
        max_length = max(max_length, len(val))
    return [val.rjust(max_length, '0') if val else None for val in str_vals]

rjust_udf = F.udf(lambda z: rjust(z), ArrayType(StringType()))

spark.read.option("header", "true").json("s3://udacity-data-engineering-nanodegree-capstone-project/data/apr_drg_codes.json")\
    .select("DRG", "Long Description", "MDC", "Type")\
    .withColumn("code", rjust_udf(to_array_udf(F.col("DRG"))))\
    .withColumn("description", to_array_udf(F.col("Long Description")))\
    .withColumn("mdc_code", rjust_udf((to_array_udf(F.col("MDC")))))\
    .withColumn("type", to_array_udf(F.col("Type")))\
    .select("code", "description", "mdc_code", "type")\
    .withColumn("zipped", F.arrays_zip("code", "description", "mdc_code", "type"))\
    .withColumn("exploded", F.explode("zipped"))\
    .select("exploded.code", "exploded.description", "exploded.mdc_code", "exploded.type")\
    .write.mode("overwrite")\
    .parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/apr_drg_code.parquet")