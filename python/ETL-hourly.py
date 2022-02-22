from bs4 import BeautifulSoup
import requests
import numpy as np
import re
import pandas as pd
import datetime
import os
from os.path import isfile, join
import calmap
import argparse
import datetime
from pathlib import Path


def str_date(date):
    return datetime.datetime.strptime(date,"%d/%m/%Y %H:%M:%S")

def date_date(date):
    return date.strftime("%d/%m/%Y")

def day_date(date):
    return date.day

def month_date(date):
    return date.month

def year_date(date):
    return date.year

def hour_date(date):
    return date.hour

def weekday_date(date):
    return date.isoweekday()

def weekend_date(date):
    return "Weekend" if date.isoweekday()> 5 else "Weekday"

def pm_10q(pm10):
    pm10 = pd.to_numeric(pm10, errors='coerce')
    if (pm10 <= 20): 
        return "Buena"
    if (pm10 >20 and pm10 <=40 ):
        return "Razonablemente buena"
    if (pm10 >40 and pm10 <=50 ):
        return "Regular"
    if (pm10 >50 and pm10 <=100 ):
        return "Desfavorable"
    if (pm10 >100 and pm10 <=150 ):
        return "Muy desfavorable"
    if (pm10 >150):
        return "Extremandamente desfavorable"

def quality(pm10):
    pm10 = pd.to_numeric(pm10, errors='coerce')
    if (pm10 <= 50): 
        return "GOOD"
    if (pm10 >50 ):
        return "BAD"
    
def range_time(hour):
    if (hour >= 6  and  hour <= 18 ):
        return "6-18"
    if (hour >=19 or hour <= 5 ):
        return "19-5"
    
def tidy_data(data):
    data.columns = ['fecha', 'station', 'limit', 'NO2', 'NOX','O3','PM10']

    data['FULL_DATE'] = data['fecha'].apply(str_date)
    data['DATE'] = data['FULL_DATE'].apply(date_date)
    data['DAY'] = data['FULL_DATE'].apply(day_date)
    data['MONTH'] = data['FULL_DATE'].apply(month_date)
    data['YEAR'] = data['FULL_DATE'].apply(year_date)
    data['HOUR'] = data['FULL_DATE'].apply(hour_date)
    data['WEEKDAY'] = data['FULL_DATE'].apply(weekday_date)
    data['WEEKEND'] = data['FULL_DATE'].apply(weekend_date)
    data['PM10_Q'] = data['PM10'].apply(pm_10q)
    data['QUALITY'] = data['PM10'].apply(quality)
    data['RANGE'] = data['HOUR'].apply(range_time)

    data = data[['FULL_DATE','DATE','DAY','MONTH','YEAR','HOUR','WEEKDAY','WEEKEND','O3','NO2','NOX','PM10','PM10_Q','QUALITY','RANGE']]
    data = data.replace('---', np.nan)
    return data


def main():
    currentDateTime = datetime.datetime.now()
    date = currentDateTime.date()
    current_year = date.strftime("%Y")
    current_month = date.strftime("%m")

    parser = argparse.ArgumentParser(prog='ETL-daily')
    parser.add_argument('--start', nargs='?', default=str(int(current_month)), help='start month for the query')
    parser.add_argument('--end', nargs='?', default=str(int(current_month)+1), help='end month for the query')
    parser.add_argument('--year', nargs='?', default=current_year, help='year for the query')

    args = parser.parse_args()

    start_month=args.start
    final_month=args.end

    output_file="san-basilio"+"-mean-hourly-"+start_month+"-"+final_month+"-"+current_year+".csv"
    filepath = Path('data/'+output_file)  

    url="https://sinqlair.carm.es/calidadaire/obtener_datos.aspx?tipo=tablaRedVigilancia"

    data_dates={
        'tipoConsulta': "medias_horarias",
        'estacionesSelec': "e_sanbasilio_San Basilio",
        'contaminantesSelec': "NO2-NOX*NOx-O3-PM10",
        'periodoConsulta': "sel_rango",
        'tipo_dato': "grid"
    }

    headers= {
        'Host': "sinqlair.carm.es",
        'User-Agent': "Mozilla/5.0 Gecko/20100101 Firefox/70.0",
        'Accept': "*/*",
        'Accept-Language': "en-US,en;q=0.5",
        'Accept-Encoding': "gzip, deflate, br",
        'X-Requested-With': "XMLHttpRequest",
        'Content-Type': "application/x-www-form-urlencoded; charset=UTF-82",
        'Content-Length': "114",
        'Origin': "https://sinqlair.carm.es",
        'DNT': "1",
        'Connection': "keep-alive",
        'Referer': "https://sinqlair.carm.es/calidadaire/estaciones/estacion.aspx?San%20Basilio",
        'Cookie': "ASP.NET_SessionId=qhnag2eedjorexs450mudas1"
    }

    df = pd.DataFrame()

    for x in range(int(start_month), int(final_month)):
        
        fechaInicioConsulta= "01/" + str(x) + "/" + args.year
        if (x==12):
            fechaFinConsulta = "01/" + "01" + "/" + str(int(args.year)+1)
        else:
            fechaFinConsulta= "01/" + str(x+1) + "/" + args.year

        data_dates['fechaInicioConsulta'] = fechaInicioConsulta
        data_dates['fechaFinConsulta'] = fechaFinConsulta
        print(fechaInicioConsulta)
        print(fechaFinConsulta)

        r = requests.post(url, data=data_dates, headers=headers)
        data = r.text.replace("ï»¿","")
        data = data.replace("\r\n","")

        data = pd.read_json(data)
        df = df.append(data, ignore_index=True)


    df = tidy_data(df)
    df.to_csv(filepath, index=None, header=True)

if __name__== "__main__":
    main()
