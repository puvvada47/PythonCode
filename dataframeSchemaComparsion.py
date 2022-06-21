
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import lit

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

fixedSchema = ["age","name","salary","org","dep2"]
f1=lambda DF,col: DF.withColumn(col,lit("Null"))
df: DataFrame= spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load(
    "C:/Users/KPUVVAD/Desktop/emp.csv")
df.show()
dfSchema=df.columns


diff=set(fixedSchema).difference(set(dfSchema))
dfDiff=set(dfSchema).difference(set(fixedSchema))

print("diff: ",diff)
print("dfdiff: ",dfDiff)



if len(dfDiff)>0:
    df=df.drop(*(dfDiff))
    df.show()
if len(diff)>0:
    [df := f1(df, x) for x in diff]
    df.show()








# [df.withColumn(
#    key,
#    when(
#        lit(has_column(df, key),
#        key
#    ).otherwise(lit(None).cast("string")) for key in diff]



#f2=lambda df,e : df.drop(e)











