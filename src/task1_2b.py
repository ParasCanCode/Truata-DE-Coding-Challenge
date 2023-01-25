# Part 1 Task 2b

#Import Spark Module
from pyspark import SparkContext

#Create a Spark Context Object
sc = SparkContext.getOrCreate()

#File Path 
path = 'C:/Users/Paras/Desktop/Truata/groceries.csv' 

#Output Directory Path
outputDir = 'C:/Users/Paras/Desktop/Truata/out/'

#Load CSV data
groceryList = sc.textFile(path).map(lambda x: x.split(','))

#Using FaltMap and Distinct to create a list of all distinct products 
distinctGroceryList = groceryList.flatMap(lambda x : x).distinct().collect()

#Count of product
productCount = len(distinctGroceryList)

#Save into Text File
with open(outputDir + 'out_1_2b.txt','w') as outputFile:
    outputFile.write("Count: {}".format(productCount))