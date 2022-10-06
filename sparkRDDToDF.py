import findspark
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

findspark.init()
sc = SparkContext()
sparkSession = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


#------------------------------------------
# #converting rdd to dataframe
#------------------------------------------
Employee=Row('id','name','age')
samRDD=sc.textFile(name='C:/Users/KPUVVAD/Desktop/sample.txt')
lineArrayRDD=samRDD.map(lambda line:line.split(","))
dataRDD=lineArrayRDD.map(lambda lineArray:Employee(*lineArray))
list=dataRDD.collect()
print(list)
df=sparkSession.createDataFrame(dataRDD)
df.show(truncate=False)
# ------------------------------------------




# -------------------------------------------------------------
# creating the dataframe from List
# -------------------------------------------------------------

list = [('Alice', 1)]
df = sparkSession.createDataFrame(list)
df.toDF("name", "id")

# -------------------------------------------------------------





deptList = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptRDD = sc.parallelize(deptList)
# list=deptRDD.collect()


# -------------------------------------------------------------
# creating dataframe from RDD
# -------------------------------------------------------------
df2 = sparkSession.createDataFrame(deptRDD).toDF("name", "id")
df2.printSchema()
df2.show(truncate=False)
# -------------------------------------------------------------





# -------------------------------------------------------------
# creating dataframe from RDD
# -------------------------------------------------------------
deptColumns = ["name", "id"]
df2 = sparkSession.createDataFrame(deptRDD).toDF(*(deptColumns))
df2.printSchema()
df2.show(truncate=False)
# -------------------------------------------------------------






# -------------------------------------------------------------
# creating dataframe from RDD using Row
# -------------------------------------------------------------

dept = Row("name", "id")
deptRowRDD = deptRDD.map(lambda list: dept(*list))
df2 = sparkSession.createDataFrame(deptRowRDD)
df2.printSchema()
df2.show(truncate=False)

# -------------------------------------------------------------





# -------------------------------------------------------------
# creating dataframe from RDD using explict schema
# -------------------------------------------------------------


# creating the dataframe using rdd and list of schema
deptColumns = ["name", "id"]
deptDF = sparkSession.createDataFrame(deptRDD, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
# -------------------------------------------------------------






# -------------------------------------------------------------
# creating the dataframe from RDD using  structtype schema
# -------------------------------------------------------------


deptSchema = StructType([
    StructField('name', StringType(), True),
    StructField('id', LongType(), True)
])

deptDF1 = sparkSession.createDataFrame(deptRDD, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

# -------------------------------------------------------------







# >>> l = [('Alice', 1)]
# >>> spark.createDataFrame(l).collect()
# [Row(_1='Alice', _2=1)]

# >>> spark.createDataFrame(l, ['name', 'age']).collect()
# [Row(name='Alice', age=1)]

# >>> d = [{'name': 'Alice', 'age': 1}]

# >>> spark.createDataFrame(d).collect()
# [Row(age=1, name='Alice')]

# >>> rdd = sc.parallelize(l)
# >>> spark.createDataFrame(rdd).collect()
# [Row(_1='Alice', _2=1)]

# >>> df = spark.createDataFrame(rdd, ['name', 'age'])
# >>> df.collect()
# [Row(name='Alice', age=1)]

# >>> from pyspark.sql import Row
# >>> Person = Row('name', 'age')
# >>> person = rdd.map(lambda r: Person(*r))
# >>> df2 = spark.createDataFrame(person)
# >>> df2.collect()
# [Row(name='Alice', age=1)]
