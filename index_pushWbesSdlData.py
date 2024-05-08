import requests
import json
import datetime
from src.appconfig import loadAppConfig
import logging
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
logging.basicConfig(filename="files/pushWbesData.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 # Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


current_date = datetime.date.today()
# Format the current date as "YYYY-MM-DD" and "DD-MM-YYYY"
formatted_date = current_date.strftime("%Y-%m-%d")
formatted_date1 = current_date.strftime("%d-%m-%Y")
formatted_date2= current_date.strftime("%d%m%Y")
wbesApiUser = appConfig['wbesApiUserName']
wbesApiPass = appConfig['wbesApiPass']
wbesTxtUrl = f"https://wbes.wrldc.in:85/scada_{formatted_date2}.txt"
wbesTxtDf= pd.read_csv(wbesTxtUrl, header=None, sep='\s+')
wbesTxtDf.set_index(0, inplace=True)

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

for singleGenData in staticDataListt:
    if singleGenData["utility_type"]=='Regional_IPP' or singleGenData["utility_type"]=='ISGS':
        genAcr=singleGenData["wbes_acronym"]
        wbesTxtGenAcr=appConfig.get(genAcr)
        genAcr = genAcr.replace(', ',',')
        genAcr= genAcr.replace('&','%26')
        plantId= singleGenData["plant_id"]
        logger.info("----------------New Gen --------------------")
        logger.info(f"{genAcr} with plantId {plantId} processing Started")

        if singleGenData["wbes_acronym"] != 'NA':
            wbesApiUrl = f"https://wbes.wrldc.in/WebAccess/GetFilteredSchdData?USER={wbesApiUser}&PASS={wbesApiPass}&DATE={formatted_date1}&ACR={genAcr}"
            
            try:
                response = requests.get(wbesApiUrl)
                wbesApiRespData = json.loads(response.text)
                groupWiseDataList = wbesApiRespData['groupWiseDataList']
                decList = wbesApiRespData['decList']
                sumSdl=[0 for i in range(96)]
                sumDc=[0 for i in range(96)]
                for gendata in groupWiseDataList:
                    sdlList=gendata['netScheduleSummary']['NET_Total'].split(',')
                    for ind in range(len(sumSdl)):
                        sumSdl[ind] = sumSdl[ind] + round(-1 * float(sdlList[ind]))
                
                # for ipp no dc is provided hence , hence checking for only ISGS gen
                if(len(decList)>0):
                    for gendata in decList:
                        dcList=gendata['SellerDCTotal'].split(',')
                        for ind in range(len(sumDc)):
                            sumDc[ind] = sumDc[ind] + round(float(dcList[ind]))
                # in case of IPP reading wbes.txt file to get DC that is Pmax
                else:
                    wbesTextGenAcrList = [acr.strip() for acr in wbesTxtGenAcr.split(',')]
                    for singleGenAcr in wbesTextGenAcrList:
                        pmaxAcr= singleGenAcr+'_PMAX'
                        ippDcList=wbesTxtDf.loc[pmaxAcr, :].values.tolist()   
                        for ind in range(len(sumDc)):
                            sumDc[ind] = sumDc[ind] + round(float(ippDcList[ind]))     
                apiRespObj = {
                    "plant_id": plantId,
                    "dc": sumDc,
                    "qsold": [0 for i in range(96)],
                    "schedule": sumSdl,
                    "data_date": formatted_date,
                }
                genRespList.append(apiRespObj)
                logger.info(f"{genAcr} processing done with data= {apiRespObj}")
                
            except Exception as err:
                logger.info(f"{genAcr} processing Error with error = {err}")
                failedGeneratorRespList.append(genAcr)

try:
    data_json = json.dumps(genRespList)
    response = requests.post(posturl, data=data_json, headers=headers, verify=False)
    logger.info(f"Wbes data post done with response {response} and text {response.text}")
except Exception as err:
    logger.error(f"Wbes data post unsuccessfull with response {response} and text {response.text}")

logger.info(f"failed generator list is {failedGeneratorRespList}")

# print(data_json)
# 

