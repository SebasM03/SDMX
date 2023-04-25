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
SELECT 
	indicator.indicatorID
	,indicator.indicatorNameES
	,indicator.dataDomainID
	,indicator.categoryID
	,indicator.indicatorPropriety
	,indicator.FLARID
	,indicator.indicatorNameESShort
	,indicatorLeyendNameES
	,GETDATE() AS currentDate
    ,'correo@flar.net'  AS email
FROM indicator
WHERE indicator.indicatorID = 'NGDP_PA_XDC'
;"""
#Llarmas datos de la base de datos
sqlheader=dbu.excecute_sql(sqlHeader)
#Crear DataFrame con Pandas
dc = pd.DataFrame(sqlheader)

#Consulta DataSet
sqlDataSet="""
SELECT DISTINCT 
    indicator.indicatorID
	,serie.freqID
	,serie.refAreaID
	,time_format.timeFormatID
	,serie.counterpartArea
	,unit_multiplier.unitMultID
	,unit.unitID
    ,indicator.categoryID
    ,indicator.indicatorPropriety
    ,indicator.indicatorNameESShort
FROM observation_value
LEFT OUTER JOIN serie ON observation_value.serieID =  serie.serieID
LEFT OUTER JOIN unit_multiplier ON serie.unitMultID = unit_multiplier.unitMultID
LEFT OUTER JOIN unit ON serie.unitID = unit.unitID
LEFT OUTER JOIN indicator ON serie.indicatorID =  indicator.indicatorID
LEFT OUTER JOIN time_format ON serie.timeFormatID = time_format.timeFormatID
LEFT OUTER JOIN observation_status ON observation_value.obsStatusID = observation_status.obsStatusID
WHERE indicator.indicatorID = 'NGDP_PA_XDC'
ORDER BY serie.refAreaID ASC
;"""

sqldataset=dbu.excecute_sql(sqlDataSet)
df = pd.DataFrame(sqldataset)


dc["indicatorID"] = dc["indicatorID"].astype(str)
dc["FLARID"] = dc["FLARID"].astype(str)
dc["currentDate"] = dc["currentDate"].astype(str)

#Quitar espacios en los campos
dc["ID"] = dc["indicatorID"].str.split(" ", n=1).str[0]
dc["FLARID"] = dc["FLARID"].str.split(" ", n=1).str[0]
df["refAreaID"] = df["refAreaID"].str.split(" ", n=1).str[0]
df["timeFormatID"] = df["timeFormatID"].str.split(" ", n=1).str[0]


root = ET.Element("MessageGroup")
root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
root.set("xsi:schemaLocation", "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message https://flar.com/sie/")


#Se crea elemento Header
header = ET.SubElement(root, "Header")

for x, row in dc.iterrows():
    ID = ET.SubElement(header, "ID")
    ID.text = str(row["ID"])
    Name = ET.SubElement(header, "Name")
    Name.text = str(row["indicatorLeyendNameES"])
    test = ET.SubElement(header, "Test")
    test.text = "false"
    prepared = ET.SubElement(header, "Prepared")
    prepared.text = str(row["currentDate"])
    sender = ET.SubElement(header, "Sender", id=str(row["FLARID"]))
    name = ET.SubElement(sender, "Name", lang="es")
    name.text = "FLAR - Fondo LatinoAmericano de Reservas"
    contact = ET.SubElement(sender, "Contact")
    department = ET.SubElement(contact, "Department", lang="es")
    department.text = "FLAR - Gerencia Tecnología y Estudios Económicos"
    uri = ET.SubElement(contact, "URI",  url="https://flar.com/sie/", email=str(row["email"]))
    extracted = ET.SubElement(header, "Extracted")
    extracted.text = str(row["currentDate"])
    dataset = ET.SubElement(root, "DataSet", action="Replace", category=str(row["categoryID"]), propiety=str(row["indicatorPropriety"]), nameshort=str(row["indicatorNameESShort"]))

#Consulta Series
sql="""
SELECT
    observation_value.timePeriod
	,observation_value.obsValue
	,observation_status.obsStatusName
	,serie.refAreaID
FROM observation_value
LEFT OUTER JOIN serie ON observation_value.serieID =  serie.serieID
LEFT OUTER JOIN unit_multiplier ON serie.unitMultID = unit_multiplier.unitMultID
LEFT OUTER JOIN unit ON serie.unitID = unit.unitID
LEFT OUTER JOIN indicator ON serie.indicatorID =  indicator.indicatorID
LEFT OUTER JOIN time_format ON serie.timeFormatID = time_format.timeFormatID
LEFT OUTER JOIN observation_status ON observation_value.obsStatusID = observation_status.obsStatusID
WHERE indicator.indicatorID = 'NGDP_PA_XDC'
ORDER BY serie.refAreaID, observation_value.timePeriod ASC
;"""


#Llamar datos de la base de datos
result=dbu.excecute_sql(sql)
#Crear DataFrame con Pandas
ds = pd.DataFrame(result)
ds["refAreaID"] = ds["refAreaID"].str.split(" ", n=1).str[0]

obs_dict = {}
for x, row in df.iterrows():
    series = ET.SubElement(dataset, "Series", freqID=str(row["freqID"]), refAreaID=str(row["refAreaID"]), timeFormat=str(row["timeFormatID"]), counterpartArea=str(row["counterpartArea"]), unit=str(row["unitID"]))
    
    # Filtrar el DataFrame por refAreaID actual
    filtered_ds = ds[ds["refAreaID"] == row["refAreaID"]]

    # Recorrer el DataFrame filtrado y crear elementos "Obs" dentro de la etiqueta "Series"
    for i, row in filtered_ds.iterrows():
        values = ET.SubElement(series, "Obs", timePeriod=str(row["timePeriod"]), obsValue=str(row["obsValue"]), obsStatusName=str(row["obsStatusName"]), refAreaID=str(row["refAreaID"]))

arbol = ET.ElementTree(root)
arbol.write("SDMX.xml", encoding="utf-8", xml_declaration=True)
print(arbol)
