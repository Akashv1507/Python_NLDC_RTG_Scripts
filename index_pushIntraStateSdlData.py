import requests
import json
import datetime as dt
from src.appconfig import loadAppConfig
import logging
import psycopg2
import pandas as pd
appConfig = loadAppConfig()

token_url = "https://rtgapi.grid-india.in/sendData/api-token-auth/"
getStaticDataurl = "https://rtgapi.grid-india.in/sendData/generator/filtered_details?region_name=WRLDC"
posturl = "https://rtgapi.grid-india.in/sendData/wbes-data/saveupdatemulti/"

credentials = {
    'username': appConfig['userName'],
    'password': appConfig['password']
}
# Create and configure logger
logging.basicConfig(filename="files/pushInraStateSdlData.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 # Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


currDate = dt.datetime.now()
# Format the current date as "YYYY-MM-DD"
currentDateStr = currDate.strftime("%Y-%m-%d")
startTime= currDate.replace(hour=0, minute=0, second=0, microsecond=0)
startTimeStr= startTime.strftime("%Y-%m-%d %H:%M:%S")
endTime= currDate.replace(hour=23, minute=59, second=0, microsecond=0)
endTimeStr= endTime.strftime("%Y-%m-%d %H:%M:%S")

response = requests.post(token_url, data=credentials, verify=False)
if response.status_code == 200:
    # Token was obtained successfully
    token_data = response.json()
    token = token_data.get("token")
    logger.info(f"Token: {token}")
else:
    # Token retrieval failed
    logger.error("Token retrieval failed.")

headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}


responseAcr = requests.get(getStaticDataurl, headers=headers, verify=False)
staticDataListt = json.loads(responseAcr.text)

genRespList=[]
failedGeneratorRespList = []

# fetching Schedule and DC data for intrastate generator
dbConn = None
dbCur = None
isInsertSuccess = True
schDf= pd.DataFrame()
dcDf= pd.DataFrame()
try:
    dbConn = psycopg2.connect(host=appConfig['db_host'], dbname=appConfig['db_name'],
                                user=appConfig['db_username'], password=appConfig['db_password'])  
    schFetchSql = "select date_time, plant_name, sum(sch_data)as sch_val from intraday_sch_data where date_time>=%(start_time)s and date_time<=%(end_time)s group by date_time, plant_name order by date_time"
    dcFetchSql= "select date_time, plant_name, sum(dc_data)as dc_val from intraday_dc_data where date_time>=%(start_time)s and date_time<=%(end_time)s group by date_time, plant_name order by date_time"
    schDf = pd.read_sql_query(schFetchSql, con=dbConn, params= {'start_time': startTimeStr, 'end_time': endTimeStr})
    dcDf = pd.read_sql_query(dcFetchSql, con=dbConn, params= {'start_time': startTimeStr, 'end_time': endTimeStr})
except Exception as err:
    isInsertSuccess = False
    logger.error('Error while fetching DC/SCH data and error is {err} ')
    print(err)
finally:
    if dbConn is not None:
        dbConn.close()

for singleGenData in staticDataListt:
    if (singleGenData["utility_type"]=='State' or singleGenData["utility_type"]=='State_IPP') and singleGenData["wbes_acronym"] != 'NA':
        try:
            sdlList=[]
            dcList=[]
            currentTime= startTime
            genAcrList = [stateAcr.strip() for stateAcr in  singleGenData["wbes_acronym"].split('$')]
            filteredGenSchDf = schDf[schDf['plant_name'].isin(genAcrList)]
            filteredGenDcDf = dcDf[dcDf['plant_name'].isin(genAcrList)]
            while currentTime<endTime:
                currTimeStr=currentTime.strftime("%Y-%m-%d %H:%M:%S")
                timeFilteredSchDf= filteredGenSchDf[filteredGenSchDf['date_time']==currTimeStr]
                timeFilteredDcDf= filteredGenDcDf[filteredGenDcDf['date_time']==currTimeStr]
                if len(timeFilteredSchDf.index)>0:
                    sdlList.append(timeFilteredSchDf['sch_val'].sum())
                else:
                    sdlList.append(0)
                if len(timeFilteredDcDf.index)>0:
                    dcList.append(timeFilteredDcDf['dc_val'].sum())
                else:
                    dcList.append(0)
                
                currentTime= currentTime + dt.timedelta(minutes=15)
            plantId= singleGenData["plant_id"]
            logger.info("----------------New Gen --------------------")
            logger.info(f"{genAcrList} with plantId {plantId} processing Started")

            apiRespObj = {
                "plant_id": plantId,
                "dc": dcList,
                "qsold": [0 for i in range(96)],
                "schedule": sdlList,
                "data_date": currentDateStr,
            }
            genRespList.append(apiRespObj)
            logger.info(f"{genAcrList} processing done with data= {apiRespObj}")    
        except Exception as err:
            logger.info(f"{genAcrList} processing Error with error = {err}")
            failedGeneratorRespList.append(genAcrList)

try:
    data_json = json.dumps(genRespList)
    response = requests.post(posturl, data=data_json, headers=headers, verify=False)
    logger.info(f"Intrastate Dc and Schedule data post done with response {response} and text {response.text}")
except Exception as err:
    logger.error(f"Intrastate Dc and Schedule data post unsuccessfull with response {response} and text {response.text}")
    

logger.info(f"failed generator list is {failedGeneratorRespList}")



