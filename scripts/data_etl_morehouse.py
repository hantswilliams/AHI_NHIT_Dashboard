#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 10:35:15 2020


COVID
https://nhit-odp.data.socrata.com/COVID-19/CDC-Provisional-COVID-19-Death-Counts-in-the-Unite/tg9j-vqnx

Medical Disparities
https://nhit-odp.data.socrata.com/COVID-19/CMS-Medicare-Disparities/t4a3-z56z

Internet Access
https://nhit-odp.data.socrata.com/Telehealth/ACS-Telehealth-Variables/bbux-k9wu

Vulnerability Access
https://nhit-odp.data.socrata.com/Social-Determinants-of-Health/CDC-Social-Vulnerability-Index-2018-/ippk-x3af




@author: hantswilliams
"""





import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
import re
import json


# Load Lat Long 
zip_latlong = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/us-zip-code-latitude-and-longitude.csv")
zip_latlong = zip_latlong[['Zip',
                             'City',
                             'State',
                             'Latitude',
                             'Longitude']]



# def string_to_dict(dict_string): #this is the function required to convert json to dictionary for parsing
#     dict_string = dict_string.replace("'", '"')
#     return json.loads(dict_string)


# def geocodetojson(dataframe, columnname):  #this function deals with formatting to json, then converting to a dictionary type
#     dataframe[columnname] = dataframe[columnname].astype(str)
#     dataframe[columnname] = dataframe[columnname].str.replace("'", '"')
#     dataframe[columnname] = dataframe[columnname].str.replace(r'(\[)', r'"\1')
#     dataframe[columnname] = dataframe[columnname].str.replace(r'(\])', r'\1"')
#     dataframe[columnname] = dataframe[columnname].apply(string_to_dict)



#COVID-DEATHS
original_covid = pd.read_json("https://nhit-odp.data.socrata.com/resource/tg9j-vqnx.json?$limit=5000")
geocodetojson(original_covid,"geocoded_column")
covid_temp = pd.json_normalize(original_covid['geocoded_column'])
clean_covid = covid_temp.merge(original_covid, how='left', left_index=True, right_index=True)
clean_covid = clean_covid.drop(columns=['geocoded_column'])
covid_location = clean_covid['coordinates'].str.strip('[]').str.split(', ', expand=True).rename(columns={0:'Latitude', 1:'Longitude'}) 
clean_covid = clean_covid.merge(covid_location, how='left', left_index=True, right_index=True)
clean_covid = clean_covid.drop(columns=['type', 'coordinates'])
clean_covid = clean_covid.add_prefix('covid_')
clean_covid['nhit_id_zip'] = clean_covid['covid_fips_county_code']
clean_covid = clean_covid.drop(columns=['covid_:@computed_region_cnz6_8gps', 'covid_:@computed_region_9bv7_e982',
                                        'covid_fips_county_code'])


#DISPARITIES
original_disparities = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/Population_Report%202018_June_2020_COUNTY.csv")
clean_disparities = original_disparities.add_prefix('disparities_')
clean_disparities['nhit_id_zip'] = clean_disparities['disparities_FIPS']
clean_disparities = clean_disparities.drop(columns=['disparities_FIPS'])










#VULNERABILITY
original_vulnerability = pd.read_json("https://nhit-odp.data.socrata.com/resource/ippk-x3af.json?$limit=50000")
clean_vulnerability = original_vulnerability.drop(columns=['shape'])
clean_vulnerability = clean_vulnerability.add_prefix('vulnerability_')
clean_vulnerability['nhit_id_zip'] = clean_vulnerability['vulnerability_fips']






#INTERNET ACCESS
original_internet = pd.read_json("https://nhit-odp.data.socrata.com/resource/bbux-k9wu.json?$limit=50000")
geocodetojson(original_internet,"internal_point")
internet_temp = pd.json_normalize(original_internet['internal_point'])
clean_internet = internet_temp.merge(original_internet, how='left', left_index=True, right_index=True)
clean_internet = clean_internet.drop(columns=['internal_point'])
internet_location = clean_internet['coordinates'].str.strip('[]').str.split(', ', expand=True).rename(columns={0:'Latitude', 1:'Longitude'}) 
clean_internet = clean_internet.merge(internet_location, how='left', left_index=True, right_index=True)
clean_internet = clean_internet.drop(columns=['type', 'coordinates'])
clean_internet = clean_internet.add_prefix('internet_')
clean_internet['internet_name'] = clean_internet['internet_name'].str.encode('ascii', 'ignore').str.decode('ascii') #this line is important for saving our version to sql 
clean_internet['nhit_id_zip'] = [x.split('0500000US')[1] for x in clean_internet['internet_geo_id']]
clean_internet['nhit_id_zip'] = clean_internet['nhit_id_zip'].astype(int)








#MERGE TOGETHER index 
merge_covid = clean_covid.set_index('nhit_id_zip')
merge_disparities = clean_disparities.set_index('nhit_id_zip')
merge_vulnerability = clean_vulnerability.set_index('nhit_id_zip')
merge_internet = clean_internet.set_index('nhit_id_zip')

merge1 = merge_internet.merge(merge_vulnerability, how='left', left_on='nhit_id_zip', right_on='nhit_id_zip')
merge2 = merge1.merge(merge_disparities, how='left', left_on='nhit_id_zip', right_on='nhit_id_zip')
merge3 = merge2.merge(merge_covid, how='left', left_on='nhit_id_zip', right_on='nhit_id_zip')

merge3['nhit_id_zip'] = merge3.index
merge3 = merge3.drop(columns=['nhit_id_zip'])
merge3 = merge3.reset_index()

merge3.to_csv('/Users/hantswilliams/Downloads/morehouse_demo.csv')






####SAVE TO DB FOR BACKUP 
MYSQL_HOSTNAME = '34.204.170.21'
MYSQL_USER = 'dba'
MYSQL_PASSWORD = 'ahi2020'
MYSQL_DATABASE = 'nhit'

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'
engine = create_engine(connection_string)

merge3.to_sql("morehouse_demo", con=engine, if_exists='replace')











