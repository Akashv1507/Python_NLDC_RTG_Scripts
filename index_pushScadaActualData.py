import requests
import json
import datetime as dt
from src.appconfig import loadAppConfig
import logging
from scadaApiFetcher import ScadaApiFetcher
import pandas as pd


appConfig = loadAppConfig()

token_url = "https://rtgapi.grid-india.in/sendData/api-token-auth/"
getStaticDataurl = "https://rtgapi.grid-india.in/sendData/generator/filtered_details?region_name=WRLDC"
posturl="https://rtgapi.grid-india.in/sendData/scada-data/saveupdatemulti/"

credentials = {
    'username': appConfig['userName'],
    'password': appConfig['password']
}
# Create and configure logger
logging.basicConfig(filename="files/pushScadaActualData.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 # Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

currDate = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
# Format the current date as "YYYY-MM-DD"
currentDateStr = currDate.strftime("%Y-%m-%d")

response = requests.post(token_url, data=credentials, verify=False)
if response.status_code == 200:
    # Token was obtained successfully
    token_data = response.json()
    token = token_data.get("token")
    logger.info(f"Token: {token}")
else:
    # Token retrieval failed
    logger.error("Token retrieval failed.")
    exit()

headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}

responseAcr = requests.get(getStaticDataurl, headers=headers, verify=False)
staticDataListt = json.loads(responseAcr.text)

scadaActDataListForAllGen = []
failedGeneratorRespList = []
#creating object of ScadaApiFetcher class 
obj_scadaApiFetcher = ScadaApiFetcher(appConfig['tokenUrl'], appConfig['apiBaseUrl'], appConfig['clientId'], appConfig['clientSecret'])

for singleGenData in staticDataListt:
    if singleGenData["scada_point"] != 'NA':
        currGenActDf= pd.DataFrame(columns=['value'])
        plantName = singleGenData['plant_name']
        scadaPointList = [scadaId.strip() for scadaId in singleGenData["scada_point"].split(',')]
        plantId= singleGenData["plant_id"]
        logger.info("----------------New Gen --------------------")
        logger.info(f"{scadaPointList}-{plantName} with plantId {plantId} processing Started") 
        try:
            for scadaPoint in scadaPointList:
                # fetching secondwise data from api for each entity(timestamp,value) and converting to dataframe
                resData = obj_scadaApiFetcher.fetchData(scadaPoint, currDate, currDate)
                if len(resData)>0:
                    actGenValDf = pd.DataFrame(resData, columns =['timestamp','value'])
                    #actGenValDf = actGenValDf.resample('1min', on='timestamp').agg({'value': 'first'})  # this will set timestamp as index of dataframe
                    actGenValDf = actGenValDf.resample('5min', on='timestamp').mean()
                    actGenValDf['value']=actGenValDf['value'].abs()
                    currGenActDf['value'] = pd.concat([currGenActDf.value, actGenValDf.value], axis=1).sum(axis=1)
            

            actualGenValuesList=currGenActDf['value'].to_list()
            #this will happen if scada point is present but that fetches no value or empty list
            if len(actualGenValuesList)==0:
                actualGenValuesList=[ 0 for i in range(0,288)]
            apiRespObj = {
                "plant_id": plantId,
                "actual_gen":actualGenValuesList,
                "data_date": currentDateStr,
            }
            scadaActDataListForAllGen.append(apiRespObj)
            logger.info(f"{scadaPointList} processing done with data= {apiRespObj}")
        except Exception as err:
            logger.info(f"{scadaPointList} processing Error with error = {err}")
            failedGeneratorRespList.append(scadaPointList)
try:
    data_json = json.dumps(scadaActDataListForAllGen)
    response = requests.post(posturl, data=data_json, headers=headers, verify=False)
    logger.info(f"Scada Actual Data post done with response {response} and text {response.text}")
except Exception as err:
    logger.error(f"Scada Actual Data post unsuccessfull with response {response} and text {response.text}")
    

logger.info(f"failed generator scada point list is {failedGeneratorRespList}")

# print(data_json)
# 

