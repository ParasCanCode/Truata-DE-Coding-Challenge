#Part 2 Task 2

#Import Modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Create a spark session
spark = SparkSession.builder.appName("AirBnB").getOrCreate()

#Dataset path
path = 'C:/Users/Paras/Desktop/Truata/airbnbdata.parquet'

#Output Directory Path
outputDir = 'C:/Users/Paras/Desktop/Truata/out/'

#Load parquet file into dataframe
airbnb_Data = spark.read.parquet(path)

#Aggregating Price column
priceDetail = airbnb_Data.agg(F.min('price'), F.max('price'), F.count('price'))

#Renaming Columns
renamedPriceDetails = priceDetail.withColumnRenamed('min(price)', 'min_price')\
    .withColumnRenamed('max(price)', 'max_price')\
    .withColumnRenamed('count(price)', 'row_count')

#Saving as CSV file
renamedPriceDetails.toPandas().to_csv(outputDir+'out_2_2.csv', index= False)