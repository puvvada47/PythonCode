# Importing Libraries
import shutil
from datetime import datetime
import dbutils
from Tools.scripts.dutree import display
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

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


dict={}
pipe="|"

spark = SparkSession.builder.master("local[*]").appName('SparkByExamples.com')



zipList = ["SEXPR_POS_CONTRIBUTION_TB_20220506_153606096.zip", "SEXPR_POS_CONTRIBUTION_TB_20220606_153606096.zip",
           "SEXPR_ALM_20220606_153607374.zip", "SEXPR_ALM_20220506_153607374.zip", "SEXPR_CREDIT_20220506_153607374.zip","SEXPR_IMP_20220506_171430785.zip"]
# Business Logic
# zipLocation="/mnt/dstgcpmld/jdata/zeb2bi_report"
# SEXPR_POS_CONTRIBUTION_TB_20220506_153606096.zip
# SEXPR_ALM_20220506_153607374.zip
#SEXPR_CREDIT_20220506_153607374.zip
#SEXPR_IMP_20220506_171430785.zip
print("Zip2Bi path: ", zipPath)
for zipFile in zipList:

        zipName = zipFile
        print("file Name : " + zipName)
        if zipName.__contains__("POS_CONTRIBUTION_TB"):
            if len(zipName.split("_")) > 4:
                zipDate = zipName.split("_")[4]
                if zipDate in dict:
                    zipString = dict.__getitem__(zipDate)
                    zipString = zipString + pipe + zipName
                    dict[zipDate] = zipString
                else:
                    dict[zipDate] = zipName
                    print(dict[zipDate])

        elif zipName.__contains__("ALM"):
            if len(zipName.split("_")) > 2:
                zipDate = zipName.split("_")[2]
                if zipDate in dict:
                    zipString = dict.__getitem__(zipDate)
                    zipString = zipString + pipe + zipName
                    dict[zipDate] = zipString
                else:
                    dict[zipDate] = zipName

        elif zipName.__contains__("IMP"):
            if len(zipName.split("_")) > 2:
                zipDate = zipName.split("_")[2]
                if zipDate in dict:
                    zipString = dict.__getitem__(zipDate)
                    zipString = zipString + pipe + zipName
                    dict[zipDate] = zipString
                else:
                    dict[zipDate] = zipName

        elif zipName.__contains__("CREDIT"):
            if len(zipName.split("_")) > 2:
                zipDate = zipName.split("_")[2]
                if zipDate in dict:
                    zipString = dict.__getitem__(zipDate)
                    zipString = zipString + pipe + zipName
                    dict[zipDate] = zipString
                else:
                    dict[zipDate] = zipName
        else:
                print("not related zip: " + zipName)

print(dict)


# for key in dict.keys():
#     print(key)
#     zipArray=dict[key].split("|")
#     if(len(zipArray)==2):
#         for zip in zipArray:
#             print("zip: ",zip)
#     print(dict[key])
