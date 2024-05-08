import pandas as pd
import requests
import json
import datetime

# Get the current date
current_date = datetime.date.today()

# # Format the current date as "YYYY-MM-DD"
formatted_date = current_date.strftime("%d-%m-%Y")

# print(formatted_date)

scadaData = pd.read_excel(f"{scada_file}")
geturl = "https://rtgapi.nrldc.in/sendData/generator/filtered_details/"
posturl="https://rtgapi.nrldc.in/sendData/scada-data/saveupdatemulti/"
token_url = "https://rtgapi.nrldc.in/sendData/api-token-auth/"
credentials = {
    'username': '',
    'password': ''
}

response = requests.post(token_url, data=credentials)
if response.status_code == 200:
    # Token was obtained successfully
    token_data = response.json()
    token = token_data.get('token')
    print(f'Token: {token}')
else:
    # Token retrieval failed
    print('Token retrieval failed.')

headers = {'Content-Type': 'application/json','Authorization': f'Token {token}'}

df = pd.DataFrame(scadaData)

response = requests.get(geturl,headers = headers)
data = json.loads(response.text)

plantMap = []
data_date = df["Timestamp"][0].split("T")[0]

for d in data:
    temp={"plant_id":d['plant_id']}
    temp["actual_gen"] = [round(x) for x in df[d['scada_point']].tolist()]
    temp["data_date"] = data_date
    plantMap.append(temp)


data_json = json.dumps(plantMap)

#headers = {'Content-Type': 'application/json'}
print((data_json))

response = requests.post(posturl,data=data_json,headers=headers)


print(response)

#print(plantMap)





