import requests
import json
import datetime
from src.appconfig import loadAppConfig
import logging
import sys
import pandas as pd
import math


appConfig = loadAppConfig()


token_url = "https://rtgapi.grid-india.in/sendData/api-token-auth/"

credentials = {
    'username': appConfig['userName'],
    'password': appConfig['password']
}
# Create and configure logger
logging.basicConfig(filename="files/updateStaticData.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 # Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

response = requests.post(token_url, data=credentials, verify=False)
if response.status_code == 200:
    # Token was obtained successfully
    token_data = response.json()
    token = token_data.get("token")
    logger.info(f"Token: {token}")
else:
    # Token retrieval failed
    logger.error("Token retrieval failed.")
    sys.exit()
headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}


staticDataDf = pd.read_csv("files/GasPointsUpdated.csv")
staticDataList = staticDataDf.values.tolist()

modifiedAllGenList = []
for singlePlantData in staticDataList:
    # converting number to 5 digit no
    logger.info('---------------------------------new generating stsn---------------------------------')
    plantId = 'RTG_WR' + f'{singlePlantData[0]:05d}'
    patchurl = f"https://rtgapi.grid-india.in/sendData/generator/process/{plantId}/"
    plantName = singlePlantData[1]

    fuelType=''
    if singlePlantData[2]== 'COAL':
        fuelType = 'THERMAL'
    elif singlePlantData[2]== 'HYDRO':
        fuelType = 'HYDEL'
    elif singlePlantData[2]== 'LNG':
        fuelType = 'GAS'
    elif singlePlantData[2]== 'NUCLEAR':
        fuelType = 'NUCLEAR'
    elif singlePlantData[2]== 'LIGNITE':
        fuelType = 'LIGNITE'
    else:
        continue

    # discarding all generator of other regions and continuing loop
    if singlePlantData[3] != 'WEST REGION':
        continue
    
    stateName= ''
    if singlePlantData[4]=='CHHATTISGARH':
        stateName ='Chhattisgarh'
    if singlePlantData[4]=='GOA':
        stateName ='Goa'
    if singlePlantData[4]=='GUJARAT':
        stateName ='Gujarat'
    if singlePlantData[4]=='MADHYA PRADESH':
        stateName ='Madhya Pradesh'
    if singlePlantData[4]=='MAHARASHTRA':
        stateName ='Maharashtra'

    utilityType= ''
    if singlePlantData[5]=='ISGS':
        utilityType='ISGS'
    elif singlePlantData[5]=='REGIONAL_IPP':
        utilityType='Regional_IPP'
    elif singlePlantData[5]=='STATE_OWNED':
        utilityType='State'
    elif singlePlantData[5]=='STATE_IPP':
        utilityType='State_IPP'
    
    owner = singlePlantData[6]
    insCapacity= int(singlePlantData[7])
    effectiveCapacity= int(singlePlantData[8])


    wbesAcr= singlePlantData[9]
    if str(wbesAcr)=='nan':
        wbesAcr='NA'

    scadaId= singlePlantData[10]
    if str(scadaId)=='nan':
        scadaId='NA'
        
    updationData = {
        'plant_id':plantId,
        'plant_name':plantName,
        'fuel_type':fuelType,
        'region_name':'WRLDC',
        'state_name':stateName,
        'utility_type':utilityType,
        'wbes_acronym':wbesAcr,
        'owner_name':owner,
        'installed_capacity':insCapacity,
        'effective_capacity':effectiveCapacity,
        'scada_point':scadaId
        }
    modifiedAllGenList.append(updationData)
    try:
        updationDataJson = json.dumps(updationData)
        response = requests.patch(patchurl, headers=headers, data =updationDataJson, verify=False)
        logger.info(f"Updation done with response {response} and text {response.text}")
    except Exception as err:
        logger.error(f"Error while updation and err is {err}")
# pd.DataFrame(modifiedAllGenList).to_excel("files/ModifiedStaticDataToNrldc.xlsx", index=False)

