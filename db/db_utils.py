# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 11:09:58 2023

@author: SEB
"""
import json
import pyodbc
import pandas as pd

with open("./config/config.json", 'r', encoding="utf-8") as f:
    data = json.load(f)


server=data["server"]
db=data["db"]
user=data["user"]
pwd=data["pwd"]


def _get_connection():
    str_conn='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+pwd+';Trusted_Connection=no;'
    print(str_conn)
    # Crea una conexion por pyodbc
    connection = pyodbc.connect(str_conn)
    return connection
    
    
def excecute_sql(sql):
    conn= _get_connection()    
    dr_result = pd.read_sql_query(sql,conn)
    return  dr_result