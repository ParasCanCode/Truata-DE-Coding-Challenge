#Part 1 Task 3

#Import Spark Module
from pyspark import SparkContext
from operator import add

#Create a Spark Context Object
sc = SparkContext.getOrCreate()

#File Path 
path = 'C:/Users/Paras/Desktop/Truata/groceries.csv' 

#Output Directory Path
outputDir = 'C:/Users/Paras/Desktop/Truata/out/'

#Load CSV data
groceryList = sc.textFile(path).map(lambda x: x.split(','))

# Create a flatten rdd of all items
flatGroceryList = groceryList.flatMap(lambda x : x)

#List of tuple of products and product frequnecy in descending order 
productCountTuple = flatGroceryList.map(lambda x: (x ,1)).reduceByKey(add).sortBy(lambda x : x[1], ascending = False).collect()

#Saving Top 5 products based on frequency in Text file
with open(outputDir + 'out_1_3.txt','w') as outputFile:
    for element in productCountTuple[0:5]:
        outputFile.write("{}\n".format(element))


