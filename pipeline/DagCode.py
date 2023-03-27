import datetime as dt
from lib2to3.pgen2.pgen import DFAState

import requests
import json
import pprint
import pandas as pd

import datetime
import gspread
import sys
import time as tm

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def WebScraping(ti):

    api_url = 'http://air4thai.pcd.go.th/services/getNewAQI_JSON.php'
    
    data_info = requests.get(api_url)
    info = json.loads(data_info.text)

    api_url_ROAD = "https://traffic.longdo.com/api/json/traffic/index?callback=callback_function"
    data_info_ROAD = requests.get(api_url_ROAD)

    date = info['stations'][0]['LastUpdate']['date']
    time = str(int(info['stations'][0]['LastUpdate']['time'][:2])-1)
    if(int(time) == -1):
        time = str(0)
    station = info['stations'][0]['stationID']

    li = []

    for st in info['stations'][:5]:
        station = st['stationID']
        LAT = st['lat']
        LONG = st['long']
        api_url ="http://air4thai.com/forweb/getHistoryData.php?stationID="+station+"&param=WS,WD,TEMP,RH,PM10,PM25&type=hr&sdate="+date+"&edate="+date+"&stime="+time+"&etime="+time
        print(api_url)
        data_info = requests.get(api_url)
        if data_info.text[10:12] == "Er": continue
        tm.sleep(2)
        info = json.loads(data_info.text)

        Temp = info['stations'][0]['data'][0]['TEMP']
        if Temp == None: Temp=25.0
        Temp = float(Temp)
        
        RH = info['stations'][0]['data'][0]['RH']
        if RH == None: RH=45.0
        RH = float(RH)

        WD = info['stations'][0]['data'][0]['WD']
        if WD == None: WD=1.0
        WD = float(WD)
        
        WS = info['stations'][0]['data'][0]['WS']
        if WS == None: WS=50.0
        WS = float(WS)
        
        PM10 = info['stations'][0]['data'][0]['PM10']
        if PM10 == None: PM10=20.0
        PM10 = float(PM10)

        PM25 = info['stations'][0]['data'][0]['PM25']
        if PM25 == None: continue

        index_ROAD = float(data_info_ROAD.text[27:30])

        # สิ่งที่จะส่งไปอีก module
        sublist = [date,time,station,Temp,WD,WS,RH,PM10,PM25,index_ROAD,LAT,LONG]
        #print(sublist,"\n")
        li.append(sublist)

    ti.xcom_push(key ='PushData',value = li)
    print("Model Here Sent : ",li)
        
def Predict_Model(ti):
    
    st_list = ti.xcom_pull(key = 'PushData',task_ids='WebScraping')

    Result = []
    

    for li in st_list:
        if(len(li[1]) == 1):
            Date = li[0] + ' 0' + li[1] + ':00'
        else:
            Date = li[0] + ' ' + li[1] + ':00'

        Year = int(Date[:4])
        Month = int(Date[5:7])
        Day = int(Date[8:10])
        Hour = int(Date[11:13])


        d = {"Date":[Date],"Year":[Year],"Month":[Month],"Day":[Day],"Hour":[Hour],"Station":[li[2]],"PM10":[li[7]],"PM2.5":[li[8]],"Wind speed":[li[5]],"Wind dir":[li[4]],"Temp":[li[3]],"Rel hum":[li[6]],"index":[li[9]],"LAT":[li[10]],"LONG":[li[11]]} 
        df = pd.DataFrame(data=d)

        df['Temp40'] = df['Temp'] // 40
        df['Hour'] = df['Hour'] * 10

        #Formula 1
        df['YrForecast'] = 1899.0644713031472 + (-0.92844995)*df['Year']
        df['PM2.5/Yr'] = df['PM2.5'] / df['YrForecast']

        #Formula 2
        df['MForecast'] = 1.3264339028048946 + -0.05080388*df['Month']
        df['PM2.5/M'] = df['PM2.5'] / df['MForecast']

        #Formula 3
        c0 = 20.437409965348824
        c1 = -0.007932379515054476
        c2 = 0.6000160281647597
        c3 = -0.5166667678529522
        c4 = -0.004644518158401889
        c5 = -0.494973892243922
        c6 = -0.07766773410748254
        c7 = -0.44488973350359173
        # ขอตาต้า
        c8 = 0.587705582596042


        df['PM2.5f'] = c0 + c1*df['Hour'] + c2*df['PM10'] + c3*df['Wind speed'] + c4*df['Wind dir'] + c5*df['Temp'] + c6*df['Rel hum'] + c7*df['index']  + c8*df['Temp40']  
        print(df)

        r = list(df.iloc[0])
        Real = [str(i) for i in r]

        Result.append(Real)

    ti.xcom_push(key ='PushResult',value = Result)
    
    print("Model Here Recieve : ",Result)



    
def Send_GoogleSheet(ti):

    Result = ti.xcom_pull(key = 'PushResult',task_ids='Predict_Model')


    for r in Result:
        sa = gspread.service_account(filename="/home/poon/airflow/dags/keen-philosophy.json")
        sh = sa.open("ForPowerBi")
        wks = sh.worksheet("dfdate1")
        wks.append_row(r)

    print("Google Colab To excel Not Happen Yet")
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 10, 1, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}
with DAG('DataSciencePM2.5',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/45 * * * *',
         # schedule_interval=None,
         ) as dag:
        opr_parse_recipes = PythonOperator(task_id='WebScraping',do_xcom_push = False,python_callable=WebScraping)
        opr_download_image = PythonOperator(task_id='Predict_Model',do_xcom_push = False,python_callable=Predict_Model)
        opr_store_data = PythonOperator(task_id='Send_GoogleSheet',do_xcom_push = False,python_callable=Send_GoogleSheet)

opr_parse_recipes >> opr_download_image >> opr_store_data
