import pandas as pd
import os
import requests
import xmltodict
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import subprocess
from dotenv import load_dotenv
from connection import *


def getData(date):
    dbCon = connect()
    myCursor = dbCon.cursor()
    myCursor.execute("SELECT IP_USER,TELEFONO_RADIUS AS TELEFONO,DROPID_PLANTA,MODEM_SERIE_RADIUS AS MODEM_SERIE FROM RADIUS_BDMI_"+date)
    data = pd.DataFrame(myCursor.fetchall())
    data.columns = myCursor.column_names
    return data

def getBDCT(clauseIn):
    dbCon = connectCloud("bdci")
    myCursor = dbCon.cursor()
    myCursor.execute("SELECT TELEFONO,TECNOLOGIA,MODEM_MODELO,PAQUETE_CALIDAD,PERFIL_RADIUS,QUEJAS_1S,QUEJAS_4S,QUEJAS_6M,VEL_CONF_DN,VEL_CONF_UP,DISTRITO,AREA,COPE FROM sem2412 WHERE TELEFONO IN("+clauseIn+")")
    data = pd.DataFrame(myCursor.fetchall())
    data.columns = myCursor.column_names
    return data

def searchCarrier(ip, tcp,flag):
    if(flag):
        url = os.environ("urlCARRIER")+"?ipAdress="+str(ip)+"&portNumber="+str(tcp).replace(".0","")
        response = requests.get(url)
        json_response = xmltodict.parse(response.content)
        ip_user = json_response['SERV']['IP']
    else:
        ip_user = ip
    return ip_user

def generateClause(sample):
    aux = ""
    length = len(sample)
    i = 0
    for phone in sample:
        if(i == (length -1)): 
            aux += "'"+phone+"'"
        else:
            aux += "'"+phone+ "'," 
        i = i + 1
    
    return str(aux).replace('"','').replace("[","").replace("]","")

def insertLog(date,deadline,process):
    dbCon = connect81()
    mycursor = dbCon.cursor()
    query = "INSERT INTO kpi (deadline_date, real_date, delay, process) VALUES(%s,%s,ROUND(TIMESTAMPDIFF(second,%s,%s)/(3600*1.0),2),%s)"
    values = (deadline,date,deadline,date,process)
    mycursor.execute(query, values)
    dbCon.commit()
    print("Inserción correcta")

load_dotenv()
path = os.getcwd()
date = str(datetime.now() - timedelta(days=1)).split(" ")[0]
months = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre", "diciembre"]
dateAux = date.split("-")
year = int(dateAux[0])
digitMonth = int(dateAux[1])-1
month = months[digitMonth]
dateAux = datetime(int(dateAux[0]),int(dateAux[1]),int(dateAux[2]))
dateRadius = str(dateAux + timedelta(days=1)).split(" ")[0].replace("-","")
print(dateRadius)
nameFile = "FixedNetworkPerformance_"+date+".csv"
nameFileSesitive = "SensitiveData_"+date+".csv"
pathExtractSensitive = os.path.join(path,"Extract",nameFileSesitive)
pathExtractFixed = os.path.join(path,"Extract",nameFile)

columns = ['TS_RESULT','ID_RESULT','GUID_RESULT','ATTR_PROVIDER_NAME','ATTR_PROVIDER_NAME_COMMON','ID_PLATFORM_X','ID_CONNECTION_TYPE','ATTR_PLACE_NAME','ATTR_PLACE_COUNTRY','ATTR_PLACE_REGION','ATTR_PLACE_SUBREGION','ATTR_PLACE_POSTAL_CODE','ATTR_WIFI_MAC_MANUFACTURER','ATTR_WIFI_FREQUENCY_MHZ','IS_WIFI_5GHZ','VAL_DOWNLOAD_MBPS','VAL_UPLOAD_MBPS','VAL_LATENCY_MIN_MS','VAL_LATENCY_IQM_MS','VAL_LATENCY_MAX_MS','VAL_MULTISERVER_LATENCY_MS','VAL_DOWNLOAD_LATENCY_MIN_MS','VAL_DOWNLOAD_LATENCY_IQM_MS','VAL_DOWNLOAD_LATENCY_MAX_MS','VAL_UPLOAD_LATENCY_MIN_MS','VAL_UPLOAD_LATENCY_IQM_MS','VAL_UPLOAD_LATENCY_MAX_MS','NUM_PACKET_LOSS_SENT','NUM_PACKET_LOSS_RECEIVED','VAL_JITTER_MS','VAL_MULTISERVER_JITTER_MS','ATTR_DEVICE_MODEL','ATTR_DEVICE_MANUFACTURER','ATTR_SERVER_NAME','ATTR_SERVER_SPONSOR_NAME','ATTR_SERVER_LATITUDE','ATTR_SERVER_LONGITUDE','IS_SERVER_AUTO_SELECTED','IS_PORTAL_INCLUDED','ATTR_PORTAL_CATEGORIES','ATTR_SIM_OPERATOR_COMMON_NAME','ATTR_DEVICE_IP_ADDRESS_PRE','ATTR_NETWORK_IPV4_ADDRESS_Y','ATTR_DEVICE_REMOTE_PORT','ATTR_LOCATION_LATITUDE_Y','ATTR_LOCATION_LONGITUDE_Y']
dfSensitive = pd.read_csv(pathExtractSensitive,delimiter=',',header='infer',encoding="latin-1")
dfData = pd.read_csv(pathExtractFixed,delimiter=',',header='infer',encoding="latin-1")
dfData = dfData.merge(dfSensitive, on='guid_result', how='left')
dfData.columns = map(str.upper, dfData.columns)
dfData = dfData[columns]
dfData.columns = dfData.columns.str.replace('ATTR_NETWORK_IPV4_ADDRESS_Y', 'IP_USER')
dfData.columns = dfData.columns.str.replace('ATTR_DEVICE_REMOTE_PORT', 'TCP_PORT')
dfData.columns = dfData.columns.str.replace('ATTR_LOCATION_LATITUDE_Y', 'LATITUDE')
dfData.columns = dfData.columns.str.replace('ATTR_LOCATION_LONGITUDE_Y', 'LONGITUDE')
dfData.columns = dfData.columns.str.replace('ID_PLATFORM_X', 'ID_PLATFORM')

dfRadius = getData(dateRadius)
data = dfData.merge(dfRadius, on='IP_USER', how='left')
data = data.drop_duplicates(subset=['GUID_RESULT'],keep="first")
print("TERMINADO RADIUS.....")

data['FLAG_CALCULATOR'] = data.apply(lambda row : True if len(str(row['IP_USER'])) > 3 and len(str(row['TELEFONO'])) <= 3  else False,axis=1)
data['IP_CARRIER'] = data.apply(lambda row: searchCarrier(row['IP_USER'],row['TCP_PORT'],row['FLAG_CALCULATOR']),axis=1)
data = data.merge(dfRadius, left_on='IP_CARRIER',right_on ='IP_USER', how='left')
data = data.drop_duplicates(subset=['GUID_RESULT'],keep="first")
print("TERMINA CALCULADORA.....")
#data.to_csv('sample.csv', header=True, index=False)

data["TELEFONO"] = data['TELEFONO_x'].fillna(data['TELEFONO_y'])
data["DROPID_PLANTA"] = data['DROPID_PLANTA_x'].fillna(data['DROPID_PLANTA_y'])
data["MODEM_SERIE"] = data['MODEM_SERIE_x'].fillna(data['MODEM_SERIE_y'])
data = data.drop(['TELEFONO_x','TELEFONO_y','DROPID_PLANTA_x','DROPID_PLANTA_y','MODEM_SERIE_x','MODEM_SERIE_y','IP_USER_y','FLAG_CALCULATOR'],axis=1)
data.columns = data.columns.str.replace('IP_USER_x', 'IP_USER')
data['TELEFONO'] = data['TELEFONO'].astype(str)
clauseIN = generateClause(data['TELEFONO'].dropna())
dfBDCT = getBDCT(clauseIN)
data = data.merge(dfBDCT, on='TELEFONO', how='left')
data = data.assign(DATE = date)
#data.to_csv('data_complete.csv', header=True, index=False)
print("TERMINA BDCT.....")

table = "speedtest_ips_"+month+"_"+str(year)
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(host = "10.192.132.69",
                               user="Rriveror",
                               pw="DucaTI&Hypermotard$939",
                               db="speedtest"))
data.to_sql(table, con = engine, if_exists = 'append',chunksize=1000000,index=False)
print("FIN DEL PROCESO")
now = str(datetime.now()).split(".")[0]
deadline = now.split(" ")[0]+ " 22:00:00"
process = "SPEEDTEST_IPS"
insertLog(now,deadline,process)
subprocess.call(["C:\\xampp\\php\\php.exe", "C:\\xampp\\htdocs\\Webservices\\Email\\emailSpeedtest.php"])
