#Part 3 Task 2

#Import modules
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StructType, FloatType, StringType)
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

#Create a Spark Session
spark = SparkSession.builder.appName("Iris_Data").getOrCreate()

#Dataset path
path = 'C:/Users/Paras/Desktop/Truata/iris.data.csv'

#Output Directory Path
outputDir = 'C:/Users/Paras/Desktop/Truata/out/'


#Define the dataframe schema for the Iris Dataset
data_schema = [StructField('sepal_length', FloatType(), True),
               StructField('sepal_width', FloatType(), True),
               StructField('petal_length', FloatType(), True),
               StructField('petal_width', FloatType(), True),
               StructField('class', StringType(), True)]

final_schema = StructType(fields=data_schema)

#Load Iris data into dataframe
df = spark.read.csv(path, schema = final_schema)

#Replace the Dependent variable( class column) with numeric value
classindex = StringIndexer(inputCol = 'class', outputCol = 'classIndex')
final_data = classindex.fit(df).transform(df)

#Creating a class and class index dataframe for future comparison
class_label = final_data.select("classIndex", "class").distinct()

#Using Assembler to collect all columns into a single features column
assembler = VectorAssembler(inputCols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol = 'features')

#Creating a Logistic regression object
log_reg = LogisticRegression(featuresCol = 'features', labelCol = 'classIndex')

#Creating a pipeline to execute processes in order
pipeline = Pipeline(stages = [assembler, log_reg])

#Train the model
fit_model = pipeline.fit(final_data)

#Defining test values
pred_data = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2),(6.2, 3.4, 5.4, 2.3)],["sepal_length", "sepal_width", "petal_length", "petal_width"])

#Testing model against test values 
result = fit_model.transform(pred_data).select('prediction')

#Combining class label dataframe with results to get prediction class
final_result = result.join(class_label, class_label['classIndex'] == result['prediction']).select('class')

#Saving to CSV file
final_result.toPandas().to_csv(outputDir+'out_3_2.csv', index= False)