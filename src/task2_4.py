#Part 2 Task 4

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

#Identifying highest rating and lowest price
highestRating = airbnb_Data.agg(F.max('review_scores_rating')).collect()[0][0]
lowestPrice = airbnb_Data.agg(F.min('price')).collect()[0][0]

#Querying the result from Dataframe
accomodates = airbnb_Data.filter((airbnb_Data['review_scores_rating'] == highestRating) & (airbnb_Data['price'] == lowestPrice)).select('accommodates').collect()[0][0]

#Saving value in text file
with open(outputDir +'out_2_4.txt','w') as outputFile:
    outputFile.write("{}".format(accomodates))

