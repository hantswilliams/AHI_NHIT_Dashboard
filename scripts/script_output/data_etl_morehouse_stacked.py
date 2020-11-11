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
import numpy as np




#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET


def string_to_dict(dict_string): #this is the function required to convert json to dictionary for parsing	
    dict_string = dict_string.replace("'", '"')	
    return json.loads(dict_string)

def geocodetojson(dataframe, columnname):  #this function deals with formatting to json, then converting to a dictionary type	
    dataframe[columnname] = dataframe[columnname].astype(str)	
    dataframe[columnname] = dataframe[columnname].str.replace("'", '"')	
    dataframe[columnname] = dataframe[columnname].str.replace(r'(\[)', r'"\1')	
    dataframe[columnname] = dataframe[columnname].str.replace(r'(\])', r'\1"')	
    dataframe[columnname] = dataframe[columnname].apply(string_to_dict)



#COVID-DEATHS-TODATE-TOTALS
original_covid = pd.read_json("https://nhit-odp.data.socrata.com/resource/tg9j-vqnx.json?$limit=5000")
geocodetojson(original_covid,"geocoded_column")	


covid_temp = pd.json_normalize(original_covid['geocoded_column'])	
clean_covid = covid_temp.merge(original_covid, how='left', left_index=True, right_index=True)	
clean_covid = clean_covid.drop(columns=['geocoded_column'])	
covid_location = clean_covid['coordinates'].str.strip('[]').str.split(', ', expand=True).rename(columns={0:'Longitude', 1:'Latitude'}) 	
clean_covid = clean_covid.merge(covid_location, how='left', left_index=True, right_index=True)	
clean_covid = clean_covid.drop(columns=['type', 'coordinates'])

clean_covid = clean_covid.add_prefix('covid_')
clean_covid = clean_covid.drop(columns=['covid_:@computed_region_cnz6_8gps', 'covid_:@computed_region_9bv7_e982',
                                        'covid_first_week'])




#DISPARITIES
original_disparities = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/Population_Report%202018_June_2020_COUNTY.csv")
clean_disparities = original_disparities.add_prefix('disparities_')

def cleaner_tonum(dataframe, columnname):
    clean_disparities[columnname] = pd.to_numeric(clean_disparities[columnname], errors='coerce').replace(0, np.nan)

cleaner_tonum(clean_disparities,'disparities_White')
cleaner_tonum(clean_disparities,'disparities_Black')
cleaner_tonum(clean_disparities,'disparities_Asian/Pacific Islander')
cleaner_tonum(clean_disparities,'disparities_Hispanic')
cleaner_tonum(clean_disparities,'disparities_American Indian/Alaska Native')
cleaner_tonum(clean_disparities,'disparities_Other')


clean_disparities_covid = clean_disparities.merge(clean_covid, how='left', left_on='disparities_FIPS', right_on='covid_fips_county_code')
clean_disparities_covid = clean_disparities_covid[clean_disparities_covid['covid_Longitude'].notna()]








#VULNERABILITY
# For list of variables: https://data.cdc.gov/Health-Statistics/Social-Vulnerability-Index-2018-United-States-coun/48va-t53r
original_vulnerability = pd.read_json("https://nhit-odp.data.socrata.com/resource/ippk-x3af.json?$limit=50000")
clean_vulnerability = original_vulnerability.drop(columns=['shape'])
clean_vulnerability = clean_vulnerability.add_prefix('vulnerability_')
clean_vulnerability['vulnerability_county'] = clean_vulnerability['vulnerability_county'].str.upper() 

clean_vulnerability_covid = clean_vulnerability.merge(clean_covid, how='left', left_on='vulnerability_fips', right_on='covid_fips_county_code')
clean_vulnerability_covid = clean_vulnerability_covid[clean_vulnerability_covid['covid_Longitude'].notna()]









#INTERNET ACCESS
original_internet = pd.read_json("https://nhit-odp.data.socrata.com/resource/bbux-k9wu.json?$limit=50000")


geocodetojson(original_internet,"internal_point")	


internet_temp = pd.json_normalize(original_internet['internal_point'])	
clean_internet = original_internet.merge(internet_temp, how='left', left_index=True, right_index=True)	
clean_internet = clean_internet.drop(columns=['internal_point'])	
internet_location = clean_internet['coordinates'].str.strip('[]').str.split(', ', expand=True).rename(columns={0:'Latitude', 1:'Longitude'}) 	
clean_internet = clean_internet.merge(internet_location, how='left', left_index=True, right_index=True)	
clean_internet = clean_internet.drop(columns=['coordinates', 'type'])

clean_internet = clean_internet.add_prefix('internet_')
clean_internet['internet_name'] = clean_internet['internet_name'].str.encode('ascii', 'ignore').str.decode('ascii') #this line is important for saving our version to sql 
clean_internet['fips_internet_geo_id'] = [x.split('0500000US')[1] for x in clean_internet['internet_geo_id']]
clean_internet['fips_internet_geo_id'] = clean_internet['fips_internet_geo_id'].astype(int)
clean_internet = clean_internet.drop(columns=[
                                             'internet_:@computed_region_cnz6_8gps',
                                             'internet_:@computed_region_9bv7_e982'])



clean_internet_covid = clean_internet.merge(clean_covid, how='left', left_on='fips_internet_geo_id', right_on='covid_fips_county_code')
clean_internet_covid = clean_internet_covid[clean_internet_covid['covid_Longitude'].notna()]











#BRING IN CHSI Data (Health Indicators)
# chsi = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/scripts/script_output/morehouse_chsi.csv")
# chsi = chsi.drop(columns=['Unnamed: 0'])
# chsi['chsi_CHSI_County_Name'] = chsi['chsi_CHSI_County_Name'].str.upper() 
# chsi = chsi.rename(columns={
#     'chsi_CHSI_County_Name': 'County',
#     'chsi_CHSI_State_Abbr': 'State',
#     })














##SAVE LOCALLY 
clean_internet_covid.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/scripts/script_output/morehouse_covid_internet.csv')
clean_vulnerability_covid.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/scripts/script_output/morehouse_covid_vulnerability.csv')
clean_disparities_covid.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/scripts/script_output/morehouse_clean_disparities_covid.csv')







####LONG DATA 
#Disparities - stacked /4-5 rows per FIP 
#Disparities-COVID 










####SAVE TO DB FOR BACKUP 
MYSQL_HOSTNAME = '34.204.170.21'
MYSQL_USER = 'dba'
MYSQL_PASSWORD = 'ahi2020'
MYSQL_DATABASE = 'nhit'

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'
engine = create_engine(connection_string)

clean_internet_covid.to_sql("morehouse_demo_covid_internet", con=engine, if_exists='replace')
clean_vulnerability_covid.to_sql("morehouse_demo_covid_vulnerability", con=engine, if_exists='replace')
clean_disparities_covid.to_sql("morehouse_demo_covid_disparities", con=engine, if_exists='replace')











