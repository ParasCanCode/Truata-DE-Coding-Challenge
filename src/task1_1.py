#Part 1 Task 1 

#Import Spark Module
from pyspark import SparkContext

#Create a Spark Context Object
sc = SparkContext.getOrCreate()

#File Path 
path = 'C:/Users/Paras/Desktop/Truata/groceries.csv' 

#Load CSV data
groceryList = sc.textFile(path).map(lambda x: x.split(','))

#Display top 5 rows 
groceryList.take(5)