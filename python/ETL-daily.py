from bs4 import BeautifulSoup
import requests
import numpy as np
import re
import pandas as pd
import datetime
import os
from os.path import isfile, join
import argparse
import datetime
from pathlib import Path

def tidy_data(data):
    data.columns = ['fecha', 'station', 'limit', 'NO2', 'NOX','O3','PM10','PM25']

    data['FULL_DATE'] = data['fecha'].apply(str_date)
    data['DATE'] = data['FULL_DATE'].apply(date_date)
    data['DAY'] = data['FULL_DATE'].apply(day_date)
    data['MONTH'] = data['FULL_DATE'].apply(month_date)
    data['YEAR'] = data['FULL_DATE'].apply(year_date)
    data['HOUR'] = data['FULL_DATE'].apply(hour_date)
    data['WEEKDAY'] = data['FULL_DATE'].apply(weekday_date)
    data['WEEKEND'] = data['FULL_DATE'].apply(weekend_date)
    data['PM10_Q'] = data['PM10'].apply(pm_10q)
    data['PM25_Q'] = data['PM25'].apply(pm_25q)
    data['QUALITY'] = data['PM10'].apply(quality)
    
    data = data[['DATE','DAY','MONTH','YEAR','HOUR','WEEKDAY','WEEKEND','O3','NO2','NOX','PM10','PM10_Q','PM25','PM25_Q','QUALITY']]
    data = data.replace('---', np.nan)
    return data

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

def pm_25q(pm25):
    pm25 = pd.to_numeric(pm25, errors='coerce')
    if (pm25 <= 10): 
        return "Buena"
    if (pm25 >10 and pm25 <=20 ):
        return "Razonablemente buena"
    if (pm25 >20 and pm25 <=25 ):
        return "Regular"
    if (pm25 >25 and pm25 <=50 ):
        return "Desfavorable"
    if (pm25 >50 and pm25 <=75 ):
        return "Muy desfavorable"
    if (pm25 >75):
        return "Extremandamente desfavorable"

def quality(pm10):
    pm10 = pd.to_numeric(pm10, errors='coerce')
    if (pm10 <= 50): 
        return "GOOD"
    if (pm10 >50 ):
        return "BAD"

def main():

    currentDateTime = datetime.datetime.now()
    date = currentDateTime.date()
    current_year = date.strftime("%Y")

    parser = argparse.ArgumentParser(prog='ETL-daily')
    parser.add_argument('--start', nargs='?', default=current_year, help='start year for the query')
    parser.add_argument('--end', nargs='?', default=str(int(current_year)+1), help='end year for the query')

    args = parser.parse_args()

    start_year=args.start
    end_year=args.end

    output_file="san-basilio"+"-mean-daily-"+start_year+"-"+end_year+".csv"
    filepath = Path('data/'+output_file)  
    
    url="https://sinqlair.carm.es/calidadaire/obtener_datos.aspx?tipo=tablaRedVigilancia"

    data_dates={
        'tipoConsulta': "medias_diarias",
        'estacionesSelec': "e_sanbasilio_San Basilio",
        'contaminantesSelec': "NO2-NOX*NOx-O3-PM10-PM25",
        'periodoConsulta': "sel_rango",
        'tipo_dato': "grid",
        'fechaInicioConsulta': "01/01/"+start_year,
        'fechaFinConsulta': "01/01/"+end_year
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

    r = requests.post(url, data=data_dates, headers=headers)
    data = r.text.replace("ï»¿","")
    data = data.replace("\r\n","")

    data = pd.read_json(data)
    data = tidy_data(data)
    data.to_csv(filepath, index=None, header=True)
  
if __name__== "__main__":
    main()
