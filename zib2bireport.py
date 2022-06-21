# Importing Libraries
import shutil
from datetime import datetime
import dbutils
from Tools.scripts.dutree import display
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *

#variable Declaration
env="dev"


mountLoc="/mnt/{0}stgcpmld/jdata".format(env)
zipPath= "{0}/zeb2bi_zipreport".format(mountLoc)
unZipPath= "{0}/zeb2bi_unzipreport".format(mountLoc)
archiveZipPath= "{0}/zeb2bi_zipreport_Archive".format(mountLoc)
archive_format="zip"
dbfs="/dbfs"
databaseName="jdp"
creationTimeStamp =datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

posContributionTbSchema= "KEY_DATE;POS_COMPONENT_ID;POSITION_ID;POSITION_SUB_ID;CONTRIB_MARGIN_COMPONENT_ID;CONTRIB_MARG_COMP_RES_TYPE_ID;PERIODIC_CONTRIB_AMT;CONTRIB_MARGIN_RATE;CONTRIB_PV_AMT;CAPITAL_COMM_PV_AMT;VALIDITY_ID"
intPerAmtTbSchema= "KEY_DATE;BASIC_POSITION_ID;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;NOMINAL_VALUE_AMT;INTEREST_PERIOD_AMT;INTEREST_TRAFO_AMT;LIQUI_TRAFO_AMT;CONTRIBUTION_CAT_ID;CONTRIBUTION_AMT"
profitLossTbSchema= "KEY_DATE;PL_ITEM_NAME;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;PORTFOLIO_NAME;GAIN_LOSS_ID;PL_ITEM_AMT"
"KEY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_START_AMT;PV_END_AMT;PV_SIM_RISKLESS_AMT;PV_SIM_EXPECTED_AMT;PV_FUNDING_COST_AMT;TIME_TO_WALL_AMT"
riskKpiSimTbSchema= "KEY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_START_AMT;PV_END_AMT;PV_SIM_RISKLESS_AMT;PV_SIM_EXPECTED_AMT;PV_FUNDING_COST_AMT;TIME_TO_WALL_AMT"
riskKpiTbSchema= "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_BP_AMT;DURATION;HIDDEN_RESERVE_AMT;PV_AMT;RORAC_RATE;IR_SHOCK_LOSS_RATE;EQUITY_T2_AMT"
riskVarhistTbSchema= "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;HOLDING_PERIOD_ID;CONF_LVL_RATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;VAR_AMT;CONDITION_SCENARIO;PLANNING_OF_MEASURES;RASTER_ID;INTEREST_BASE_ID;RELEVENT_INTEREST_MARKETS;WITH_MIRROR_OFFSET_ID;WITH_DESIGNATED_SWAP_ID;PV_AMT;PV_FORECAST_AMT;SAMPLE_BEGIN_DATE;SAMPLE_END_DATE;SAMPLE_SIZE_AMT;SAMPLE_RASTER_AMT;DECAY_FACTOR;FOCUS_AMT;OLDEST_SCENARIO_DATE;NEWEST_SCENARIO_DATE;FORECAST_DATE;COMPARISON_TYPE;VAR_INTEREST_RISK_AMT;VAR_FX_RISK_AMT;VAR_CREDIT_SPREAD_RISK_AMT;VAR_LIQUI_SPREAD_RISK_AMT;VAR_MATURITY_TRAFO_AMT;VAR_SPREAD_TRAFO_AMT;VAR_INTEREST_TRAFO_AMT;VAR_FX_TRAFO_AMT"
riskVarhistdTbSchema= "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;HOLDING_PERIOD_ID;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;START_DATE;END_DATE;PV_AMT;CONF_LVL_CUM_AMT;WEIGHT_AMT;PL_AMT;VALUE_CHNG_INTERV_ID;VC_TOTAL_AMT;VC_RISKLESS_AMT;VC_MATURITY_TRAFO_AMT;VC_SPREAD_TRAFO_AMT;VC_FX_TRAFO_AMT;VC_INTEREST_TRAFO_AMT;VAR_INTEREST_RISK_AMT;VAR_FX_RISK_AMT;VAR_CREDIT_SPREAD_RISK_AMT;VAR_LIQUI_SPREAD_RISK_AMT;MIRRORED_OFFSET"

zeb2BiCsvToTableSchemaDict = {
  "SEXPR_POS_CONTRIBUTION_TB.csv": ["ZEB_I_POS_CONTRIBUTION_TB", posContributionTbSchema],
  "SEXPR_TDM_RISK_KPI_TB.csv": ["ZEB_I_TDM_RISK_KPI_TB", riskKpiTbSchema],
  "SEXPR_TDM_RISK_KPI_SIM_TB.csv": ["ZEB_I_TDM_RISK_KPI_SIM_TB", riskKpiSimTbSchema],
  "SEXPR_TDM_RISKVARHIST_TB.csv": ["ZEB_I_TDM_RISKVARHIST_TB", riskVarhistTbSchema],
  "SEXPR_TDM_RISKVARHISTD_TB.csv": ["ZEB_I_TDM_RISKVARHISTD_TB", riskVarhistdTbSchema],
  "SEXPR_TDM_PROFIT_LOSS_TB.csv": ["ZEB_I_TDM_PROFIT_LOSS_TB", profitLossTbSchema],
  "SEXPR_TDM_INT_PER_AMT_TB.csv": ["ZEB_I_TDM_INT_PER_AMT_TB", intPerAmtTbSchema]
}
nullToDFColumn=lambda dataframe, field : dataframe.withColumn(field, lit("Null"))

spark = SparkSession.builder.master("local[*]").appName('SparkByExamples.com')




# Business Logic
# zipLocation="/mnt/dstgcpmld/jdata/zeb2bi_report"
# SEXPR_POS_CONTRIBUTION_TB_20220506_153606096.zip
# SEXPR_ALM_20220506_153607374.zip
print("Zip2Bi path: ", zipPath)
for zippFile in dbutils.fs.ls(zipPath):
    if (zippFile.isFile):
        try:
            dbutils.fs.ls(zippFile.path)
        except Exception as e:
            print("file not found since it was already moved to archive path:")
            continue
        zippFileName = zippFile.name
        print("file Name : " + zippFileName)
        if (zippFileName.__contains__("POS_CONTRIBUTION_TB")):
            if len(zippFileName.split("_")) > 4:
                zipKeyDate = zippFileName.split("_")[4]
        elif (zippFileName.__contains__("ALM")):
            if len(zippFileName.split("_")) > 2:
                zipKeyDate = zippFileName.split("_")[2]
        else:
            print("not related zip: " + zippFileName)
        print("key date: " + zipKeyDate)
        for zipFile in dbutils.fs.ls(zipPath):
            if (zipFile.isFile):
                zipfileName = zipFile.name
                if (zippFileName != zipfileName and zipfileName.__contains__("POS_CONTRIBUTION_TB") and len(zipfileName.split("_"))>4 and len(zippFileName.split("_"))>2):
                    posZipKeyDate = zipfileName.split("_")[4]
                    if zipKeyDate == posZipKeyDate:
                        print("keydate for ALM and POS zip matching: ", zipKeyDate)
                        posTimsestamp = zipfileName.split("_")[5].split(".")[0]
                        almTImestamp = zippFileName.split("_")[3].split(".")[0]
                        posFileCreationTs = datetime.strptime(posZipKeyDate + " " + posTimsestamp,
                                                              '%Y%m%d %H%M%S%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
                        almFileCreationTs = datetime.strptime(zipKeyDate + " " + almTImestamp,
                                                              '%Y%m%d %H%M%S%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
                        creationTimeStamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                        posZipFileName = zipfileName
                        print("creationtimestamp", creationTimeStamp)
                        print("posfilecreationts", posFileCreationTs)
                        print("almfilecreationts", almFileCreationTs)
                        print("ALM zip file name: ", zippFileName)
                        print("POS zip file name: ", posZipFileName)
                        dbutils.fs.rm(unZipPath, True)
                        shutil.unpack_archive(dbfs + zipPath + "/" + zippFileName, dbfs + unZipPath, archive_format)
                        shutil.unpack_archive(dbfs + zipPath + "/" + posZipFileName, dbfs + unZipPath, archive_format)
                        dbutils.fs.mv(zipPath + "/" + zippFileName, archiveZipPath)
                        dbutils.fs.mv(zipPath + "/" + posZipFileName, archiveZipPath)

                        for dbfsFile in dbutils.fs.ls(unZipPath):
                            if zeb2BiCsvToTableSchemaDict.__contains__(dbfsFile.name):
                                print("csv file: ", dbfsFile.name)
                                print("csv file meta information: ", dbfsFile)
                                schema = zeb2BiCsvToTableSchemaDict[dbfsFile.name][1]
                                fixedSchema = schema.split(";")
                                table = zeb2BiCsvToTableSchemaDict[dbfsFile.name][0]
                                csvFilePath = unZipPath + "/" + dbfsFile.name
                                print("csv file path: ", csvFilePath)
                                tablePath = "{0}/{1}/".format(mountLoc, table)
                                print("table path : ", tablePath)
                                df = spark.read.format("com.databricks.spark.csv").option("header", "true").option(
                                    "delimiter", ";").load(csvFilePath)
                                df.persist()
                                display(df)
                                dfSchema = df.columns
                                diff = set(fixedSchema).difference(set(dfSchema))
                                dfDiff = set(dfSchema).difference(set(fixedSchema))
                                print("columns present in fixed schema but not in csv file schema: ", diff)
                                print("columns present in csv file schema but not in fixed schema: ", dfDiff)
                                if (len(dfDiff) > 0):
                                    df = df.drop(*(dfDiff))
                                if len(diff) > 0:
                                    [df := nullToDFColumn(df, field) for field in diff]
                                    display(df)
                                columns_list = df.columns
                                print("final columns in sync with fixed schema: ", columns_list)
                                for column in columns_list:
                                    df = df.withColumn(column, df[column].cast(StringType()))
                                if table == "ZEB_I_POS_CONTRIBUTION_TB":
                                    df = df.withColumn("creationts", lit(creationTimeStamp)).withColumn(
                                        "filecreationts", lit(posFileCreationTs))
                                else:
                                    df = df.withColumn("creationts", lit(creationTimeStamp)).withColumn(
                                        "filecreationts", lit(almFileCreationTs))
                                display(df)
                                df.write.format("delta").mode("append").save(tablePath)
                                createTabSqlQuery = "CREATE TABLE IF NOT EXISTS " + databaseName + "." + table + " USING DELTA LOCATION " + "'" + tablePath + "'"
                                zebDF = spark.sql(createTabSqlQuery)
                            else:
                                print("zeb2BiCsvToTableSchemaDict does not contain File Name: ",dbfsFile.name)
                        break
                    else:
                        print("keydate for ALM and POS zip not matching")