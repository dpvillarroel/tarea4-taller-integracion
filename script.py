######### SCRIPT TAREA 4 #########
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import gspread 
from gspread_dataframe import set_with_dataframe
import re

countries = ['AUS','CAN','CHE','DEU','THA','ZAF']
df_cols = ['GHO', 'COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display', 'Numeric', 'Low', 'High']

# Crear todos los archivos XML
#AUSTRALIA AUS
url_aus = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_AUS.xml'
response_aus = requests.get(url_aus)
with open('australia.xml', 'wb') as f:
    f.write(response_aus.content)
# CANADA CAN
url_can = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CAN.xml'
response_can = requests.get(url_can)
with open('canada.xml', 'wb') as f:
    f.write(response_can.content)
# SUIZA CHE
url_che = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHE.xml'
response_che = requests.get(url_che)
with open('suiza.xml', 'wb') as f:
    f.write(response_che.content)
# ALEMANIA DEU
url_deu = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_DEU.xml'
response_deu = requests.get(url_deu)
with open('alemania.xml', 'wb') as f:
    f.write(response_deu.content)
# TAILANDIA THA
url_tha = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_THA.xml'
response_tha = requests.get(url_tha)
with open('tailandia.xml', 'wb') as f:
    f.write(response_tha.content)
# SUDÁFRICA ZAF
url_zaf = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ZAF.xml'
response_zaf = requests.get(url_zaf)
with open('sudafrica.xml', 'wb') as f:
    f.write(response_zaf.content)

#Código de referencia: https://medium.com/@robertopreste/from-xml-to-pandas-dataframes-9292980b1c1c
def parse_XML(xml_file, df_cols): 
   
    xtree = ET.parse(xml_file)
    xroot = xtree.getroot()
    rows = []
    
    for node in xroot: 
        res = []
        for el in df_cols: 
            if node is not None and node.find(el) is not None:
                node_var = node.find(el).text
                
                if el == "Numeric":
                    str(node_var)
                    node_var  = re.sub('[.]',',',node_var )

                res.append(node_var)

            else: 
                res.append(None)
        rows.append({df_cols[i]: res[i] 
                     for i, _ in enumerate(df_cols)})
    
    out_df = pd.DataFrame(rows, columns=df_cols)
        
    return out_df

countries = ['AUS','CAN','CHE','DEU','THA','ZAF']

aus_df = parse_XML('australia.xml', df_cols)
can_df = parse_XML('canada.xml', df_cols)
che_df = parse_XML('suiza.xml', df_cols)
deu_df = parse_XML('alemania.xml', df_cols)
tha_df = parse_XML('tailandia.xml', df_cols)
zaf_df = parse_XML('sudafrica.xml', df_cols)



#ACCES GOOGLE SHEET
gc = gspread.service_account(filename='taller-tarea4-316614-f61660a0a11d.json')
sh = gc.open_by_key('1hqA3IsiCwZ-TrqApLZTMXwx5gKbpoC4HCPeoeI6h_XA')
worksheet = sh.get_worksheet(0)

#APPEND DATA TO SHEET
df_data = pd.concat([aus_df, can_df, che_df, deu_df, tha_df, zaf_df])
set_with_dataframe(worksheet,df_data)

