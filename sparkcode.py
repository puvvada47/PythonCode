from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime


spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \

df = spark.read.csv("C:/Users/MKONDAP/Desktop/Learning/sample.csv")
df.show()
df.createOrReplaceTempView()
k=df.head()#it will give single row if head is 1 otherwise it will give list of rows
df.rdd.collect()#it will return list
df.filter()

list=df.limit(1).collect()
if len(list)==1:
    print("data present")
else:
    print("dataframe is empty. Please check")

df.write.csv("outputCSV", escapeQuotes=True,mode="overwrite")
df.write.csv("outputCSV",quote='"',mode="overwrite")
df.write.csv("outputCSV",escape="\\")

rdd=df.rdd
list=rdd.map(lambda row: row.get(0) )
for element in list:
    type(element)
    zipDate = datetime.strptime(element, '%Y-%m-%d').date()
    break

