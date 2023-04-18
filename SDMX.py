# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 09:01:56 2023

@author: SEB
"""
import db.db_utils as dbu
import xml.etree.ElementTree as ET
import pandas as pd


#Consulta encabezado
sqlHeader="""
SELECT TOP 1
	indicator.indicatorID
	,serie.freqID
	,serie.refAreaID
	,time_format.timeFormatID
	,serie.counterpartArea
	,unit_multiplier.unitMultID
	,unit.unitID
	,indicator.FLARID
    ,GETDATE() AS currentDate
    ,'correo@flar.net'  AS email
FROM observation_value
LEFT OUTER JOIN serie ON observation_value.serieID =  serie.serieID
LEFT OUTER JOIN unit_multiplier ON serie.unitMultID = unit_multiplier.unitMultID
LEFT OUTER JOIN unit ON serie.unitID = unit.unitID
LEFT OUTER JOIN indicator ON serie.indicatorID =  indicator.indicatorID
LEFT OUTER JOIN time_format ON serie.timeFormatID = time_format.timeFormatID
LEFT OUTER JOIN observation_status ON observation_value.obsStatusID = observation_status.obsStatusID
;"""


#Llamar datos de la base de datos
sqlheadert=dbu.excecute_sql(sqlHeader)
#Crear DataFrame con Pandas
df = pd.DataFrame(sqlheadert)

df["indicatorID"] = df["indicatorID"].astype(str)
df["FLARID"] = df["FLARID"].astype(str)
df["currentDate"] = df["currentDate"].astype(str)

#Quitar espacios en los campos
df["ID"] = df["indicatorID"].str.split(" ", n=1).str[0]
df["refAreaID"] = df["refAreaID"].str.split(" ", n=1).str[0]
df["timeFormatID"] = df["timeFormatID"].str.split(" ", n=1).str[0]
df["FLARID"] = df["FLARID"].str.split(" ", n=1).str[0]



root = ET.Element("MessageGroup", xmlns="https://flar.com/sie/")

#Se crea elemento Header
header = ET.SubElement(root, "Header")

for x, row in df.iterrows():
    ID = ET.SubElement(header, "ID")
    ID.text = str(row["ID"])
    test = ET.SubElement(header, "Test")
    test.text = "false"
    prepared = ET.SubElement(header, "Prepared")
    prepared.text = str(row["currentDate"])
    sender = ET.SubElement(header, "Sender", id=str(row["FLARID"]))
    name = ET.SubElement(sender, "Name", lang="es")
    name.text = "FLAR - Fondo LatinoAmericano de Reservas"
    contact = ET.SubElement(sender, "Contact")
    department = ET.SubElement(contact, "Department", lang="es")
    department.text = "Gerencia Tecnología y Estudios Económicos - FLAR"
    uri = ET.SubElement(contact, "URI", email=str(row["email"]))
    extracted = ET.SubElement(sender, "Extracted")
    extracted.text = str(row["currentDate"])
    
    dataset = ET.SubElement(root, "DataSet", action="Replace")
    series = ET.SubElement(dataset, "Series", freqID=str(row["freqID"]), refAreaID=str(row["refAreaID"]), timeFormat=str(row["timeFormatID"]), counterpartArea=str(row["counterpartArea"]), unit=str(row["unitID"]))


#Consulta DataSet 
sql="""
SELECT TOP 78
	observation_value.timePeriod
	,observation_value.obsValue
	,observation_status.obsStatusName
FROM observation_value
LEFT OUTER JOIN serie ON observation_value.serieID =  serie.serieID
LEFT OUTER JOIN unit_multiplier ON serie.unitMultID = unit_multiplier.unitMultID
LEFT OUTER JOIN unit ON serie.unitID = unit.unitID
LEFT OUTER JOIN indicator ON serie.indicatorID =  indicator.indicatorID
LEFT OUTER JOIN time_format ON serie.timeFormatID = time_format.timeFormatID
LEFT OUTER JOIN observation_status ON observation_value.obsStatusID = observation_status.obsStatusID
;"""

#Llamar datos de la base de datos
result=dbu.excecute_sql(sql)
#Crear DataFrame con Pandas
ds = pd.DataFrame(result)

for i,row in ds.iterrows():
      values = ET.SubElement(series, "message:Values", timePeriod=str(row["timePeriod"]),obsValue=str(row["obsValue"]),obsStatusName=str(row["obsStatusName"]))
      


arbol = ET.ElementTree(root)
arbol.write("SDMX.xml", encoding="utf-8", xml_declaration=True)


