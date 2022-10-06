from pyspark.context import SparkContext
sc = SparkContext("local[*]", "test")
dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = sc.parallelize(dept)
print(rdd.getNumPartitions())
list = rdd.collect()
print(list)








# sc = SparkContext("local[*]", "test")
# tempdir="C:/Users/KPUVVAD/PycharmProjects/Testing"
# path = os.path.join(tempdir, "sample.txt")
# with open(path, "w") as testFile:
#    k= testFile.write("Hello world!")
# textFile = sc.textFile(name=path)
# list=textFile.collect()
# print(list)


