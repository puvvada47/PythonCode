import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

target = "/dbfs/mnt/dstgcpmld/jdata/zeb_control_zip_report"
dbfstargetZip = "/dbfs/mnt/dstgcpmld/jdata/zeb_control_zip_report/*.zip"
dbfstargetCsv = "/dbfs/mnt/dstgcpmld/jdata/zeb_control_zip_report/*.csv"
ziparchive = "/dbfs/mnt/dstgcpmld/jdata/zeb_control_zip_report_archive"
os.system("mv {0} {1}".format(dbfstargetZip, ziparchive))
# listTables=["E_POSITION_TB","E_POSITION_COLLATERALS_TB","E_COLLATERAL_TB","E_CUST_TB","E_BP_VALUE_MISC_POS_TB","E_PROFIT_LOSS_TB","E_BORROWER_UNIT_TB"]
listTables = ["E_PROFIT_LOSS_TB", "E_BP_VALUE_MISC_POS_TB"]
zipDateSet = set()
for table in listTables:
    df = spark.sql("SELECT MAX (EXEDATE) as maxDate FROM jdp." + table)
    rdd = df.rdd
    list = rdd.collect()
    for element in list:
        zipDate = element[0].strftime('%Y%m%d')
        zipDateSet.add(zipDate)
        break
print(zipDate)
print(zipDateSet)
if len(zipDateSet) == 1:
    print("execution date is same for all zeb reports")
    for table in listTables:
        csvInputpath = "/mnt/dstgcpmld/jdata/zeb_control_" + table + "_csv"
        dbfsCsvInputpath = "/dbfs" + csvInputpath + "/*.csv"
        zipFileName = "TDM_MBLD_" + zipDate + ".zip"
        driverZipPath = "file:/databricks/driver/" + zipFileName
        tmpPath = "dbfs:/tmp"
        tmpZipPath = tmpPath + "/" + zipFileName
        print(csvInputpath)
        print(dbfsCsvInputpath)
        print(driverZipPath)
        print(tmpPath)
        print(tmpZipPath)
        # dbutils.fs.mkdirs("dbfs:/mnt/{0}/{1}/eRDR_reports/DEX_WriteOff/".format(adlsAccountName, adlsContainerName))
        #dbutils.fs.rm("{0},true".format(csvInputpath))
        #dbutils.fs.mkdirs("{0}".format(csvInputpath))
        df = spark.sql(
            "select * FROM jdp." + table + " where EXEDATE in (SELECT MAX (EXEDATE) as maxDate FROM jdp." + table + ")")
        df \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("encoding", "UTF-8") \
            .option("sep", ";") \
            .option("quoteAll", True) \
            .csv(csvInputpath)
        os.system("cp {0} {1}".format(dbfsCsvInputpath, target))
        os.system("zip -m {0} {1}".format(zipFileName, dbfstargetCsv))
        os.system("cp {0} {1}".format(driverZipPath, tmpPath))
        # dbutils.fs.cp("file:/databricks/driver/zeb_control_zip_report.zip","dbfs:/tmp")
        os.system("cp {0} {1}".format(tmpZipPath, target))
        # dbutils.fs.cp("dbfs:/tmp/zeb_control_zip_report.zip","dbfs:/mnt/dstgcpmld/jdata/zeb_control_zip_report")
else:
    print("Please run the zeb reports before running the zip report")

