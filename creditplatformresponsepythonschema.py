from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
spark.conf.set("spark.debug.maxToStringFields","true")
df= spark.read.format("json").option("multiLine", "true").load("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/newCreditPlatformJSON/writeresponseschema/response.json")
schemaJSON=df.schema.json()
print(schemaJSON)


