import pandas as pd
import requests
from src.appconfig import loadAppConfig
import datetime as dt
import cx_Oracle
import json
from sqls.realTimeOutageFetchSql import realTimeOutageFetchSql
import logging


appConfig = loadAppConfig()


tokenUrl = "https://rtgapi.grid-india.in/sendData/api-token-auth/"
getStaticDataurl = "https://rtgapi.grid-india.in/sendData/generator/filtered_details/?region_name=WRLDC"
postUrl = "https://rtgapi.grid-india.in/sendData/outage-data/save/"
credentials = {
    'username': appConfig['userName'],
    'password': appConfig['password']
}

# Create and configure logger
logging.basicConfig(filename="files/pushOutage.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 # Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

# dateKey is date till outage is fetched
currTime = dt.datetime.now()
currTimeStr= dt.datetime.strftime(currTime, '%Y-%m-%d %H:%M:%S')
logger.info(f'--------Processing Started of Outage Request On {currTimeStr}---------------')

dbConnString = appConfig['con_string_server_db']
token=""
try:
    response = requests.post(tokenUrl, data=credentials, verify=False)
    if response.status_code == 200:
        # Token was obtained successfully
        token_data = response.json()
        token = token_data.get('token')

        logger.info(f'Token Ritrieval done. Token: {token}')
    else:
        # Token retrieval failed
        logger.error('Token retrieval failed.')
except Exception as err:
    logger.error(f'fetch token post request throws error {err}')
    exit()

# Adding token to headers
headers = {'Content-Type': 'application/json','Authorization': f'Token {token}'}
responseAcr = requests.get(getStaticDataurl, headers=headers, verify=False)
staticDataListt = json.loads(responseAcr.text)

connection= None
outageDataDf= None
try:
    connection = cx_Oracle.connect(dbConnString)
except Exception as err:
    logger.error(f'error while creating a connection/cursor { err}')
else:
    try:
        outageDataDf = pd.read_sql(realTimeOutageFetchSql, params={'targetDatetime':currTimeStr }, con=connection)
        logger.info("Outage Data Fetch Done")
    except Exception as err:
        logger.exception(f'error while executing sql query {err}' )
        if connection:
            connection.close()
finally:
    if connection:
        connection.close()
        
outageDataDf['PLANT_ID'] = outageDataDf['PLANT_ID'].apply(lambda x : 'RTG_WR' + f'{x:05d}')

for singleGenData in staticDataListt:
    
    plantId = singleGenData['plant_id']
    plantName = singleGenData['plant_name']
    logger.info(f"-----------------------------Processing started for {plantName}----------------------------")
    filteredOutageDf = outageDataDf[outageDataDf['PLANT_ID']==plantId]
    sumPlanned=0
    sumForced=0
    sumRsd=0
    respObj=None
    # if any type of outage present
    if len(filteredOutageDf.index) != 0:
        for ind in filteredOutageDf.index:
            if filteredOutageDf["SHUT_DOWN_TYPE_NAME"][ind]=="FORCED":
                sumForced= sumForced + filteredOutageDf["INSTALLED_CAPACITY"][ind]
            elif filteredOutageDf["SHUT_DOWN_TYPE_NAME"][ind]=="PLANNED" and filteredOutageDf["SHUTDOWN_TAG"][ind]=="Outage":
                sumPlanned= sumPlanned + filteredOutageDf["INSTALLED_CAPACITY"][ind]
            else:
                sumRsd= sumRsd + filteredOutageDf["INSTALLED_CAPACITY"][ind]
            respObj= {
                "rsd": sumRsd,
                "fuel_shortage": 0,
                "planned_outage": sumPlanned,
                "forced_outage": sumForced,
                "plant_id":plantId ,
                "commercial_issues": 0,
            }
    #  if no outage present for current plantid , sending 0 values   
    else:
        respObj= {
                "rsd": sumRsd,
                "fuel_shortage": 0,
                "planned_outage": sumPlanned,
                "forced_outage": sumForced,
                "plant_id":plantId ,
                "commercial_issues": 0,
            }

    try:
        data_json = json.dumps([respObj])
        logger.info(data_json)
        response = requests.post(postUrl, data=data_json, headers=headers, verify=False)
        logger.info(f"Post Request Completed for {plantName} with response {response}")
            
    except Exception as e:
        logger.exception(f"Err!!Post Request Failed for {plantName} with error {response}")
    


