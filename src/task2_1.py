#Part 2 Task 1

#Import Modules
from pyspark.sql import SparkSession

#Create a spark session
spark = SparkSession.builder.appName("AirBnB").getOrCreate()

#Dataset path
path = 'C:/Users/Paras/Desktop/Truata/airbnbdata.parquet'

#Load parquet file into dataframe
airbnb_Data = spark.read.parquet(path)
