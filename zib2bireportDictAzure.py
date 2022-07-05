# Importing Libraries
import shutil
from datetime import datetime
import dbutils
from Tools.scripts.dutree import display
from pyspark.sql import *
from pyspark.sql.functions import *

# variable Declaration
env = "dev"

mountLoc = "/mnt/{0}stgcpmld/jdata".format(env)
zipPath = "{0}/zeb2bi_zipreport".format(mountLoc)
unZipPath = "{0}/zeb2bi_unzipreport".format(mountLoc)
archiveZipPath = "{0}/zeb2bi_zipreport_Archive".format(mountLoc)
archive_format = "zip"
dbfs = "/dbfs"
databaseName = "jdp"
creationTimeStamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

posContributionTbSchema = "KEY_DATE;POS_COMPONENT_ID;POSITION_ID;POSITION_SUB_ID;CONTRIB_MARGIN_COMPONENT_ID;CONTRIB_MARG_COMP_RES_TYPE_ID;PERIODIC_CONTRIB_AMT;CONTRIB_MARGIN_RATE;CONTRIB_PV_AMT;CAPITAL_COMM_PV_AMT;VALIDITY_ID"
intPerAmtTbSchema = "KEY_DATE;BASIC_POSITION_ID;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;NOMINAL_VALUE_AMT;INTEREST_PERIOD_AMT;INTEREST_TRAFO_AMT;LIQUI_TRAFO_AMT;CONTRIBUTION_CAT_ID;CONTRIBUTION_AMT"
profitLossTbSchema = "KEY_DATE;PL_ITEM_NAME;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;PORTFOLIO_NAME;GAIN_LOSS_ID;PL_ITEM_AMT"
"KEY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_START_AMT;PV_END_AMT;PV_SIM_RISKLESS_AMT;PV_SIM_EXPECTED_AMT;PV_FUNDING_COST_AMT;TIME_TO_WALL_AMT"
riskKpiSimTbSchema = "KEY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_START_AMT;PV_END_AMT;PV_SIM_RISKLESS_AMT;PV_SIM_EXPECTED_AMT;PV_FUNDING_COST_AMT;TIME_TO_WALL_AMT"
riskKpiTbSchema = "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;PV_BP_AMT;DURATION;HIDDEN_RESERVE_AMT;PV_AMT;RORAC_RATE;IR_SHOCK_LOSS_RATE;EQUITY_T2_AMT"
riskVarhistTbSchema = "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;HOLDING_PERIOD_ID;CONF_LVL_RATE;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;VAR_AMT;CONDITION_SCENARIO;PLANNING_OF_MEASURES;RASTER_ID;INTEREST_BASE_ID;RELEVENT_INTEREST_MARKETS;WITH_MIRROR_OFFSET_ID;WITH_DESIGNATED_SWAP_ID;PV_AMT;PV_FORECAST_AMT;SAMPLE_BEGIN_DATE;SAMPLE_END_DATE;SAMPLE_SIZE_AMT;SAMPLE_RASTER_AMT;DECAY_FACTOR;FOCUS_AMT;OLDEST_SCENARIO_DATE;NEWEST_SCENARIO_DATE;FORECAST_DATE;COMPARISON_TYPE;VAR_INTEREST_RISK_AMT;VAR_FX_RISK_AMT;VAR_CREDIT_SPREAD_RISK_AMT;VAR_LIQUI_SPREAD_RISK_AMT;VAR_MATURITY_TRAFO_AMT;VAR_SPREAD_TRAFO_AMT;VAR_INTEREST_TRAFO_AMT;VAR_FX_TRAFO_AMT"
riskVarhistdTbSchema = "KEY_DATE;PARAM_SCE_COMPOSITION_NAME;HOLDING_PERIOD_ID;PORTFOLIO_HIER_NAME;PORTFOLIO_NAME;START_DATE;END_DATE;PV_AMT;CONF_LVL_CUM_AMT;WEIGHT_AMT;PL_AMT;VALUE_CHNG_INTERV_ID;VC_TOTAL_AMT;VC_RISKLESS_AMT;VC_MATURITY_TRAFO_AMT;VC_SPREAD_TRAFO_AMT;VC_FX_TRAFO_AMT;VC_INTEREST_TRAFO_AMT;VAR_INTEREST_RISK_AMT;VAR_FX_RISK_AMT;VAR_CREDIT_SPREAD_RISK_AMT;VAR_LIQUI_SPREAD_RISK_AMT;MIRRORED_OFFSET"
tdmAlmCfTbSchema = "PORTFOLIO_NAME;PORTFOLIO_HIER_NAME;KEY_DATE;CF_ELEMENT_TYPE_ID;CF_DATE;CF_VALUE_AMT;BASIC_POSITION_ID;PARAM_SCE_COMPOSITION_NAME;VALIDITY_DATE"
tdmHgbPositionTbSchema = "ASSET_LIAB_ID;BANK_ID;CLIENT_ID;KEY_DATE;POS_COMPONENT_ID;POSITION_ID;POSITION_SUB_ID;STAGE_LLP_CC_ID;STAGE_LLP_CC_REASON_DESC;GLLP_CC_AMT"
tdmCreditPortfolioTbSchema = "BANK_ID;CLIENT_ID;CONFIDENCE_LEVEL_ID;KEY_DATE;RESULT_TYPE_ID;RUN_ID;SCENARIO_ID;STATUS;AVERAGE_PD_RATE;CONF_LVL_RATE;CURRENCY_ID;ELHHI_RATE;ELHHI_MIG_RATE;EXPECTED_LIFETIME_LOSS_FX_AMT;ANALYTICAL_EL_AMT;ANALYTICAL_EL_MIG_AMT;ANALYTICAL_EL_NO_MAT_SCALE_AMT;SIMULATED_EL_AMT;SIMULATED_EL_MIG_AMT;SIMULATED_EL_SEC_REC_AMT;ES_AMT;ES_MIG_AMT;ES_SEC_REC_AMT;EAD_COLL_COLLCTRY_AMT;EAD_COLL_AMT;EAD_GROSS_AMT;EAD_COLLCTRY_AMT;EAD_AMT;HHI_RATE;GINI_RATE;LGD_RATE;NOF_BU_ANALYTICAL;NOF_BU_PASSTHROUGH;NOF_BU_RISK;NOF_BU_TOTAL;NOF_CONTRACTS_ANALYTICAL;NOF_CONTRACTS_PASSTHROUGH;NOF_CONTRACTS_RISK;NOF_CONTRACTS_TOTAL;NOF_COUNTRIES_ANALYTICAL;NOF_COUNTRIES_PASSTHROUGH;NOF_COUNTRIES_RISK;NOF_COUNTRIES_TOTAL;NOF_CUSTOMERS_ANALYTICAL;NOF_CUSTOMERS_PASSTHROUGH;NOF_CUSTOMERS_RISK;NOF_CUSTOMERS_TOTAL;QUANTILE_AMT;QUANTILE_MIG_AMT;QUANTILE_SEC_REC_AMT;RATIO_ULEL_RATE;RATIO_ULEL_MIG_RATE;STANDARD_DEVIATION_AMT;STANDARD_DEVIATION_MIG_AMT;STANDARD_DEVIATION_SEC_REC_AMT;ANALYTICAL_UL_AMT;ANALYTICAL_UL_MIG_AMT;ANALYTICAL_UL_SEC_REC_AMT;SIMULATED_UL_AMT;SIMULATED_UL_MIG_AMT;SIMULATED_UL_SEC_REC_AMT;VOLUME_AT_RISK_COLLCTRY_AMT;VOLUME_AT_RISK_AMT"

zeb2BiCsvToTableSchemaDict = {
    "SEXPR_POS_CONTRIBUTION_TB.csv": ["ZEB_I_POS_CONTRIBUTION_TB", posContributionTbSchema],
    "SEXPR_TDM_RISK_KPI_TB.csv": ["ZEB_I_TDM_RISK_KPI_TB", riskKpiTbSchema],
    "SEXPR_TDM_RISK_KPI_SIM_TB.csv": ["ZEB_I_TDM_RISK_KPI_SIM_TB", riskKpiSimTbSchema],
    "SEXPR_TDM_RISKVARHIST_TB.csv": ["ZEB_I_TDM_RISKVARHIST_TB", riskVarhistTbSchema],
    "SEXPR_TDM_RISKVARHISTD_TB.csv": ["ZEB_I_TDM_RISKVARHISTD_TB", riskVarhistdTbSchema],
    "SEXPR_TDM_PROFIT_LOSS_TB.csv": ["ZEB_I_TDM_PROFIT_LOSS_TB", profitLossTbSchema],
    "SEXPR_TDM_INT_PER_AMT_TB.csv": ["ZEB_I_TDM_INT_PER_AMT_TB", intPerAmtTbSchema],
    "SEXPR_TDM_ALMCF_TB.csv": ["ZEB_I_TDM_ALMCF_TB", tdmAlmCfTbSchema],
    "SEXPR_TDM_HGB_POSITION_TB.csv": ["ZEB_I_TDM_HGB_POSITION_TB", tdmHgbPositionTbSchema],
    "SEXPR_TDM_CREDIT_PORTFOLIO_TB.csv": ["ZEB_I_TDM_CREDIT_PORTFOLIO_TB", tdmCreditPortfolioTbSchema]

}
nullToDFColumn = lambda dataframe, field: dataframe.withColumn(field, lit("Null"))

pipe = "|"

spark = SparkSession.builder.master("local[*]").appName('SparkByExamples.com')

zipList = ["SEXPR_POS_CONTRIBUTION_TB_20220506_153606096.zip", "SEXPR_POS_CONTRIBUTION_TB_20220606_153606096.zip",
           "SEXPR_ALM_20220606_153607374.zip", "SEXPR_ALM_20220506_153607374.zip", ]

print("Zip2Bi path: ", zipPath)

# Business Logic
# sample Zips
# SEXPR_POS_CONTRIBUTION_TB_20220506_153606096.zip
# SEXPR_ALM_20220506_153607374.zip
# SEXPR_CREDIT_20220506_153607374.zip
# SEXPR_IMP_20220506_171430785.zip
stichDateZipDict = {}
for zipFile in dbutils.fs.ls(zipPath):
    zipName = zipFile.name
    print("file Name : " + zipName)
    if zipName.__contains__("POS_CONTRIBUTION_TB"):
        if len(zipName.split("_")) > 4:
            zipDate = zipName.split("_")[4]
            if zipDate in stichDateZipDict:
                zipString = stichDateZipDict.__getitem__(zipDate)
                zipString = zipString + pipe + zipName
                stichDateZipDict[zipDate] = zipString
            else:
                stichDateZipDict[zipDate] = zipName

    elif zipName.__contains__("ALM"):
        if len(zipName.split("_")) > 2:
            zipDate = zipName.split("_")[2]
            if zipDate in stichDateZipDict:
                zipString = stichDateZipDict.__getitem__(zipDate)
                zipString = zipString + pipe + zipName
                stichDateZipDict[zipDate] = zipString
            else:
                stichDateZipDict[zipDate] = zipName

    elif zipName.__contains__("IMP"):
        if len(zipName.split("_")) > 2:
            zipDate = zipName.split("_")[2]
            if zipDate in stichDateZipDict:
                zipString = stichDateZipDict.__getitem__(zipDate)
                zipString = zipString + pipe + zipName
                stichDateZipDict[zipDate] = zipString
            else:
                stichDateZipDict[zipDate] = zipName

    elif zipName.__contains__("CREDIT"):
        if len(zipName.split("_")) > 2:
            zipDate = zipName.split("_")[2]
            if zipDate in stichDateZipDict:
                zipString = stichDateZipDict.__getitem__(zipDate)
                zipString = zipString + pipe + zipName
                stichDateZipDict[zipDate] = zipString
            else:
                stichDateZipDict[zipDate] = zipName
    else:
        print("not related zip: " + zipName)

print("stichDateZipDictionary: ", stichDateZipDict)

for stichDate in stichDateZipDict.keys():
    print("stichDate: ", stichDate)
    zipArray = stichDateZipDict[stichDate].split(pipe)
    if len(zipArray) == 4:
        print("for specific KeyDate, four zips are present")
        posTimsestamp = ""
        posFileCreationTs = ""
        almTimestamp = ""
        almFileCreationTs = ""
        impTimestamp = ""
        impFileCreationTs = ""
        creditTimestamp = ""
        creditFileCreationTs = ""
        dbutils.fs.rm(unZipPath, True)
        for zip in zipArray:
            if zip.__contains__("POS_CONTRIBUTION_TB"):
                if len(zip.split("_")) > 4:
                    posTimsestamp = zip.split("_")[5].split(".")[0]
                    posFileCreationTs = datetime.strptime(stichDate + " " + posTimsestamp, '%Y%m%d %H%M%S%f').strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")

            elif zip.__contains__("ALM"):
                if len(zip.split("_")) > 2:
                    almTimestamp = zip.split("_")[3].split(".")[0]
                    almFileCreationTs = datetime.strptime(stichDate + " " + almTimestamp, '%Y%m%d %H%M%S%f').strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")

            elif zip.__contains__("IMP"):
                if len(zip.split("_")) > 2:
                    impTimestamp = zip.split("_")[3].split(".")[0]
                    impFileCreationTs = datetime.strptime(stichDate + " " + impTimestamp, '%Y%m%d %H%M%S%f').strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")

            elif zip.__contains__("CREDIT"):
                if len(zip.split("_")) > 2:
                    creditTimestamp = zip.split("_")[3].split(".")[0]
                    creditFileCreationTs = datetime.strptime(stichDate + " " + creditTimestamp,
                                                             '%Y%m%d %H%M%S%f').strftime(
                        "%Y-%m-%dT%H:%M:%S.%f")
            else:
                print("zip does not match")
            print("zip: ", zip)
            shutil.unpack_archive(dbfs + zipPath + "/" + zip, dbfs + unZipPath, archive_format)
            dbutils.fs.mv(zipPath + "/" + zip, archiveZipPath)

        creationTimeStamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

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
                persistDF = df.persist()
                display(persistDF)
                dfSchema = persistDF.columns
                diff = set(fixedSchema).difference(set(dfSchema))
                dfDiff = set(dfSchema).difference(set(fixedSchema))
                print("columns present in fixed schema but not in csv file schema: ", diff)
                print("columns present in csv file schema but not in fixed schema: ", dfDiff)
                if len(dfDiff) > 0:
                    dropDF = persistDF.drop(*(dfDiff))
                    if len(diff) > 0:
                        [finalDF := nullToDFColumn(dropDF, field) for field in diff]
                elif len(diff) > 0:
                    [finalDF := nullToDFColumn(persistDF, field) for field in diff]
                else:
                    finalDF = persistDF
                columns_list = finalDF.columns
                print("final columns in sync with fixed schema: ", columns_list)

                if table == "ZEB_I_POS_CONTRIBUTION_TB":
                    zebDF = finalDF.select(*(fixedSchema)).withColumn("creationts", lit(creationTimeStamp)).withColumn(
                        "filecreationts",
                        lit(posFileCreationTs))

                elif table == "ZEB_I_TDM_HGB_POSITION_TB":
                    zebDF = finalDF.select(*(fixedSchema)).withColumn("creationts", lit(creationTimeStamp)).withColumn(
                        "filecreationts",
                        lit(impFileCreationTs))

                elif table == "ZEB_I_TDM_CREDIT_PORTFOLIO_TB":
                    zebDF = finalDF.select(*(fixedSchema)).withColumn("creationts", lit(creationTimeStamp)).withColumn(
                        "filecreationts",
                        lit(creditFileCreationTs))

                else:
                    zebDF = finalDF.select(*(fixedSchema)).withColumn("creationts", lit(creationTimeStamp)).withColumn(
                        "filecreationts",
                        lit(almFileCreationTs))
                display(zebDF)
                zebDF.write.format("delta").mode("append").save(tablePath)
                persistDF.unpersist()
                createTabSqlQuery = "CREATE TABLE IF NOT EXISTS " + databaseName + "." + table + " USING DELTA LOCATION " + "'" + tablePath + "'"
                zeb2BiDF = spark.sql(createTabSqlQuery)
            else:
                print("zeb2BiCsvToTableSchemaDict does not contain File Name: ", dbfsFile.name)
    else:
        print("for specific KeyDate, should be four zips")
