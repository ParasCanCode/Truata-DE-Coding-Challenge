#Part 2 Task 3

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

#Querying Value from dataframe for price > 5000 and review = 10
bedBathData = airbnb_Data.filter((airbnb_Data['price'] > 5000) & (airbnb_Data['review_scores_value'] == 10)).select(["bathrooms", "bedrooms"]).agg(F.mean('bathrooms'), F.mean('bedrooms'))

#Renaming Columns
renamedBedBathData = bedBathData.withColumnRenamed('avg(bathrooms)', 'avg_bathrooms')\
    .withColumnRenamed('avg(bedrooms)', 'avg_bedrooms')

#Saving as CSV file
renamedBedBathData.toPandas().to_csv(outputDir+'out_2_3.csv', index= False)