
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


list=[]

fixedSchema = ["age","name","salary","org"]
addedColumnFn=lambda DF,col: DF.withColumn(col,lit("Null"))
df= spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load(
    "C:/Users/KPUVVAD/Desktop/emp.csv")
print("inital schema: ",df.schema)
df.show()
dfSchema=df.columns
for column in dfSchema:
    list.append(col(column).cast(StringType()))

print("list: ",list)
df=df.select(*(list))
fColumns=df.schema
print("final schema: ",fColumns)



df.withColumn(column,col(column).cast(StringType()))


diff=set(fixedSchema).difference(set(dfSchema))
dfDiff=set(dfSchema).difference(set(fixedSchema))

print("diff: ",diff)
print("dfdiff: ",dfDiff)



if len(dfDiff)>0:
    df=df.drop(*(dfDiff))
    df.show()
if len(diff)>0:
    [df := addedColumnFn(df, x) for x in diff]
    df.show()








# [df.withColumn(
#    key,
#    when(
#        lit(has_column(df, key),
#        key
#    ).otherwise(lit(None).cast("string")) for key in diff]



#f2=lambda df,e : df.drop(e)











