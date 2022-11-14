from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('medical_facility_etl').getOrCreate()
sc = spark.sparkContext

spark.read.option("header", "true").csv("s3://udacity-data-engineering-nanodegree-capstone-project/data/Health_Facility_General_Information.csv")\
    .select("Facility ID", "Facility Name", "Description", "Facility Latitude", "Facility Longitude", "Facility Zip Code")\
    .withColumnRenamed("Facility ID", "id")\
    .withColumnRenamed("Facility Name", "name")\
    .withColumnRenamed("Description", "description")\
    .withColumnRenamed("Facility Latitude", "latitude")\
    .withColumnRenamed("Facility Longitude", "longitude")\
    .withColumnRenamed("Facility Zip Code", "zip_code")\
    .write.mode("overwrite").partitionBy("description")\
    .parquet("s3://udacity-data-engineering-nanodegree-capstone-project/tables/medical_facility.parquet")
            

spark.stop()