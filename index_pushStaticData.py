import pandas as pd
import requests
from src.appconfig import loadAppConfig
import logging




appConfig = loadAppConfig()

token_url = "https://rtgapi.grid-india.in/sendData/api-token-auth/"
url = 'https://rtgapi.grid-india.in/sendData/generator/save_details/'
credentials = {
    'username': appConfig['userName'],
    'password': appConfig['password']
}

# Create and configure logger
logging.basicConfig(filename="files/pushStaticData.log",
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
    token = token_data.get('token')
    logger.info(f'Token: {token}')
else:
    # Token retrieval failed
    logger.error('Token retrieval failed.')

headers = {'Content-Type': 'application/json','Authorization': f'Token {token}'}

outageStaticDataDf = pd.read_excel('files/v1AddGeneratorData.xlsx')
outageStaticDataDf['WBES_ACR'] = outageStaticDataDf['WBES_ACR'].fillna('NA')
staticDataList = outageStaticDataDf.values.tolist()

modifiedAllGenList = []
for singlePlantData in staticDataList:
    # converting number to 5 digit no
    plantId = 'RTG_WR' + f'{singlePlantData[0]:05d}'
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
        
    static_dict = {
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
    # print(static_dict)
    modifiedAllGenList.append(static_dict)
    try:
        response = requests.post(url, json = static_dict,headers = headers, verify=False)
        logger.info(f'Processed {plantName} with status {response.text}')
    except Exception as err:
        logger.error(err)
pd.DataFrame(modifiedAllGenList).to_excel("files/ModifiedStaticDataToNrldc.xlsx", index=False)