import findspark
from pyspark import SparkContext, Row
from pyspark.sql import SparkSession

findspark.init()
sc=SparkContext()
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

Employee=Row('id','name','age')
samRDD=sc.textFile(name='C:/Users/KPUVVAD/Desktop/sample.txt')
lineArrayRDD=samRDD.map(lambda line:line.split(","))
rdd=lineArrayRDD.map(lambda lineArray:Employee(*lineArray))
list=rdd.collect()
print(list)
df=spark.createDataFrame(rdd)
df.show(truncate=False)

#---------------------------------------------------
#rdd transformations
#---------------------------------------------------
filterRDD=samRDD.filter(lambda line:line.__contains__('30'))
filterList=filterRDD.collect()
print("filter result: ",filterList)


flatMapRDD=samRDD.flatMap(lambda line:line.split(","))
flatMapList=flatMapRDD.collect()
print("flatMap result: ",flatMapList)














