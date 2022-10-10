
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


#Creating StructType schema for request data
requestSchemaStr = r'{"fields":[{"metadata":{},"name":"requests","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"requestNumber","nullable":true,"type":"string"},{"metadata":{},"name":"borrowerUnits","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"borrowerUnitRole","nullable":true,"type":"string"},{"metadata":{},"name":"borrowerUnitExposure","nullable":true,"type":{"fields":[{"metadata":{},"name":"pipelineEADAmountBU","nullable":true,"type":"double"},{"metadata":{},"name":"approvedAndActivatedEADAmountBU","nullable":true,"type":"double"}],"type":"struct"}},{"metadata":{},"name":"businessPartners","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"businessPartnerNumber","nullable":true,"type":"string"},{"metadata":{},"name":"businessPartnerRole","nullable":true,"type":"string"},{"metadata":{},"name":"keyPositionFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"businessRelationSince","nullable":true,"type":"string"},{"metadata":{},"name":"potentialMatchFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"handlingKey","nullable":true,"type":"string"},{"metadata":{},"name":"handlingKeyDate","nullable":true,"type":"string"},{"metadata":{},"name":"singleBorrowerUnitNumber","nullable":true,"type":"string"},{"metadata":{},"name":"singleBorrowerUnitMainNumber","nullable":true,"type":"string"},{"metadata":{},"name":"defaultStatus","nullable":true,"type":"string"},{"metadata":{},"name":"defaultStatusSinceDate","nullable":true,"type":"string"},{"metadata":{},"name":"worstHandlingKeyBU","nullable":true,"type":"string"},{"metadata":{},"name":"worstHandlingKeyBUDate","nullable":true,"type":"string"},{"metadata":{},"name":"foundationDate","nullable":true,"type":"string"},{"metadata":{},"name":"businessExperienceSince","nullable":true,"type":"string"},{"metadata":{},"name":"legalEntityTypeCode","nullable":true,"type":"string"},{"metadata":{},"name":"startUpCompanyFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"industrySectorCode","nullable":true,"type":"string"},{"metadata":{},"name":"firstName","nullable":true,"type":"string"},{"metadata":{},"name":"lastName","nullable":true,"type":"string"},{"metadata":{},"name":"companyName","nullable":true,"type":"string"},{"metadata":{},"name":"gender","nullable":true,"type":"string"},{"metadata":{},"name":"birthDate","nullable":true,"type":"string"},{"metadata":{},"name":"crefoNumber","nullable":true,"type":"string"},{"metadata":{},"name":"schufaRequestFeature","nullable":true,"type":"string"},{"metadata":{},"name":"addresses","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"countryCode","nullable":true,"type":"string"},{"metadata":{},"name":"streetName","nullable":true,"type":"string"},{"metadata":{},"name":"streetNumber","nullable":true,"type":"string"},{"metadata":{},"name":"postalCode","nullable":true,"type":"string"},{"metadata":{},"name":"addressType","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"phoneNumbers","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"phoneNumber","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"schufaDisclosure","nullable":true,"type":{"fields":[{"metadata":{},"name":"schufaScore","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaScoreAnswer","nullable":true,"type":"string"},{"metadata":{},"name":"schufaScoreBereich","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCriterias","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"schufaCriteriaType","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCriteriaCode","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCriteriaAmount","nullable":true,"type":"double"},{"metadata":{},"name":"schufaCriteriaMonthlyInstallmentAmount","nullable":true,"type":"double"},{"metadata":{},"name":"schufaCriteriaNumberOfInstallments","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaCriteriaEndDate","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCriteriaAccountNumber","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCriteriaEventDate","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"schufaFirstName","nullable":true,"type":"string"},{"metadata":{},"name":"schufaLastName","nullable":true,"type":"string"},{"metadata":{},"name":"schufaStreetName","nullable":true,"type":"string"},{"metadata":{},"name":"schufaPostalCode","nullable":true,"type":"string"},{"metadata":{},"name":"schufaCity","nullable":true,"type":"string"},{"metadata":{},"name":"schufaNumberOfNegativeCriterias","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaNumberOfPositiveCriterias","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaNumberOfCheckingAccounts","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaNumberOfCreditCards","nullable":true,"type":"integer"},{"metadata":{},"name":"schufaReportId","nullable":true,"type":"string"},{"metadata":{},"name":"schufaValidAsOfDate","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"vcDisclosure","nullable":true,"type":{"fields":[{"metadata":{},"name":"vcIndex","nullable":true,"type":"integer"},{"metadata":{},"name":"vcNegativeCriteria","nullable":true,"type":"integer"},{"metadata":{},"name":"vcFoundationDate","nullable":true,"type":"string"},{"metadata":{},"name":"vcLegalForm","nullable":true,"type":"string"},{"metadata":{},"name":"vcIndustrySectorCode","nullable":true,"type":"string"},{"metadata":{},"name":"vcAssociates","nullable":true,"type":"integer"},{"metadata":{},"name":"vcParticipations","nullable":true,"type":"integer"},{"metadata":{},"name":"vcParticipationsAndAssociates","nullable":true,"type":"integer"},{"metadata":{},"name":"vcNegativeNotesAssociatesAndParticipations","nullable":true,"type":"integer"},{"metadata":{},"name":"vcBalanceSheetTotal","nullable":true,"type":"double"},{"metadata":{},"name":"vcRegisteredCapital","nullable":true,"type":"double"},{"metadata":{},"name":"vcTurnoverAmount","nullable":true,"type":"double"},{"metadata":{},"name":"vcNumberOfEmployees","nullable":true,"type":"integer"},{"metadata":{},"name":"vcInquiryCounter28Days","nullable":true,"type":"integer"},{"metadata":{},"name":"vcInquiryCounter56Days","nullable":true,"type":"integer"},{"metadata":{},"name":"vcInquiryCounter365Days","nullable":true,"type":"integer"},{"metadata":{},"name":"vcValidAsOfDate","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"proposalAndContracts","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"contract","nullable":true,"type":{"fields":[{"metadata":{},"name":"contractNumber","nullable":true,"type":"string"},{"metadata":{},"name":"ocaNumber","nullable":true,"type":"string"},{"metadata":{},"name":"contractEndDate","nullable":true,"type":"string"},{"metadata":{},"name":"relatedContractNumber","nullable":true,"type":"string"},{"metadata":{},"name":"status","nullable":true,"type":"string"},{"metadata":{},"name":"daimlerEmployeeBusinessFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"disableAutomaticApprovalFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"eadAmount","nullable":true,"type":"double"},{"metadata":{},"name":"productCode","nullable":true,"type":"string"},{"metadata":{},"name":"iban","nullable":true,"type":"string"},{"metadata":{},"name":"leasingContractTransferFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"leasingSpecialPaymentAmount","nullable":true,"type":"double"},{"metadata":{},"name":"monthlyInstallmentNetAmount","nullable":true,"type":"double"},{"metadata":{},"name":"plannedContractDurationInMonths","nullable":true,"type":"integer"},{"metadata":{},"name":"defaultStatus","nullable":true,"type":"string"},{"metadata":{},"name":"arrearsAmount","nullable":true,"type":"double"},{"metadata":{},"name":"approvalStatus","nullable":true,"type":"string"},{"metadata":{},"name":"dealerAddress","nullable":true,"type":{"fields":[{"metadata":{},"name":"countryCode","nullable":true,"type":"string"},{"metadata":{},"name":"streetName","nullable":true,"type":"string"},{"metadata":{},"name":"streetNumber","nullable":true,"type":"string"},{"metadata":{},"name":"postalCode","nullable":true,"type":"string"},{"metadata":{},"name":"addressType","nullable":true,"type":"string"},{"metadata":{},"name":"city","nullable":true,"type":"string"}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"vehicle","nullable":true,"type":{"fields":[{"metadata":{},"name":"purchasePriceNetAmount","nullable":true,"type":"double"},{"metadata":{},"name":"marketValueAmount","nullable":true,"type":"double"},{"metadata":{},"name":"residualValueAmount","nullable":true,"type":"double"},{"metadata":{},"name":"mileageAtContractStart","nullable":true,"type":"double"},{"metadata":{},"name":"typeOfAssetLevel5","nullable":true,"type":"string"},{"metadata":{},"name":"initialRegistrationDate","nullable":true,"type":"string"},{"metadata":{},"name":"typeOfAssetBasicElement","nullable":true,"type":"string"},{"metadata":{},"name":"vehicleStatusCode","nullable":true,"type":"string"},{"metadata":{},"name":"vehicleModelFeatures","nullable":true,"type":"string"},{"metadata":{},"name":"powerUnit","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"individualDisclosure","nullable":true,"type":{"fields":[{"metadata":{},"name":"maritalStatusCode","nullable":true,"type":"string"},{"metadata":{},"name":"occupationTypeCode","nullable":true,"type":"string"},{"metadata":{},"name":"employedSince","nullable":true,"type":"string"},{"metadata":{},"name":"individualDisclosureDate","nullable":true,"type":"string"},{"metadata":{},"name":"netIncomeAmount","nullable":true,"type":"double"},{"metadata":{},"name":"totalExpensesAmount","nullable":true,"type":"double"},{"metadata":{},"name":"numberOfCohabitants","nullable":true,"type":"integer"},{"metadata":{},"name":"numberOfUnderagedChildren","nullable":true,"type":"integer"}],"type":"struct"}},{"metadata":{},"name":"corporationDisclosures","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"financialStatementDate","nullable":true,"type":"string"},{"metadata":{},"name":"financialStatementType","nullable":true,"type":"string"},{"metadata":{},"name":"businessAssignmentFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"tnwr","nullable":true,"type":"double"},{"metadata":{},"name":"ebitda","nullable":true,"type":"double"},{"metadata":{},"name":"pcdc","nullable":true,"type":"double"},{"metadata":{},"name":"sustainableDCSSurplus","nullable":true,"type":"double"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"scoringSteeringInformation","nullable":true,"type":{"fields":[{"metadata":{},"name":"scoringRequestDate","nullable":true,"type":"string"},{"metadata":{},"name":"scoringType","nullable":true,"type":"string"},{"metadata":{},"name":"portfolioSegment","nullable":true,"type":"string"},{"metadata":{},"name":"processCode","nullable":true,"type":"string"},{"metadata":{},"name":"proposalScoringDate","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"switchOffPolicyRulesFlag","nullable":true,"type":"integer"},{"metadata":{},"name":"historicalCreditDecision","nullable":true,"type":{"fields":[{"metadata":{},"name":"creditDecision","nullable":true,"type":"string"},{"metadata":{},"name":"creditDecisionDate","nullable":true,"type":"string"},{"metadata":{},"name":"leasingSpecialPaymentAmount","nullable":true,"type":"double"}],"type":"struct"}}],"type":"struct"},"type":"array"}}],"type":"struct"},"type":"array"}}],"type":"struct"},"type":"array"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"bulkRequestNumber","nullable":true,"type":"string"},{"metadata":{},"name":"identifiers","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"value","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}}],"type":"struct"}'

requestStructType = StructType.fromJson(json.loads(requestSchemaStr))

# textHandler = sc._jvm.com.daimler.nbx.pseudo.lib.handler.TextHandler()
# textHandler.setPasswordPhrase("crepl")

with open("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/creditplatform/request.json") as jsonFile:
    requestMap: object=json.load(jsonFile)
    #requestMap is dictionary each json is a Dictionary with key value pairs
if "requests" in requestMap:
    if type(requestMap["requests"]) is list:
        for request_index, request in enumerate(requestMap["requests"]):
             print(0)
             print(request)
             print(type(request))

             print(requestMap["requests"][0]["borrowerUnits"][0]["businessPartners"][
                 0]["proposalAndContracts"][0]["contract"][
                 "iban"])

             print(request["borrowerUnits"][0]["businessPartners"][
                       0]["proposalAndContracts"][0]["contract"][
                       "iban"])

             print(type(request["borrowerUnits"][0]["businessPartners"][
                       0]["proposalAndContracts"][0]["contract"]))

             df=spark.read.format("json").schema(requestStructType).load(
                 "C:/Users/KPUVVAD/Desktop/Project/Project_Modules/creditplatform/request.json")

list=df.collect()
for e in list:
    print(e)