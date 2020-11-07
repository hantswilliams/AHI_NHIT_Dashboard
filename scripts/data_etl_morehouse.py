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


# Load Lat Long 
zip_latlong = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/us-zip-code-latitude-and-longitude.csv")
zip_latlong = zip_latlong[['Zip',
                             'City',
                             'State',
                             'Latitude',
                             'Longitude']]

zip_latlong = zip_latlong.add_prefix('gpsdata_')



#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET
#######NON-STACKED DATASET



#COVID-DEATHS-TODATE-TOTALS
original_covid = pd.read_json("https://nhit-odp.data.socrata.com/resource/tg9j-vqnx.json?$limit=5000")
clean_covid = original_covid.add_prefix('covid_')
clean_covid['nhit_id_zip'] = clean_covid['covid_fips_county_code']
clean_covid = clean_covid.drop(columns=['covid_:@computed_region_cnz6_8gps', 'covid_:@computed_region_9bv7_e982',
                                        'covid_fips_county_code', 'covid_geocoded_column', 
                                        'covid_state', 'covid_county_name', 'covid_last_week', 'covid_date_as_of',
                                        'covid_first_week'])


#DISPARITIES
original_disparities = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/Population_Report%202018_June_2020_COUNTY.csv")
clean_disparities = original_disparities.add_prefix('disparities_')
clean_disparities['nhit_id_zip'] = clean_disparities['disparities_FIPS']
clean_disparities = clean_disparities.drop(columns=['disparities_FIPS'])

def cleaner_tonum(dataframe, columnname):
    clean_disparities[columnname] = pd.to_numeric(clean_disparities[columnname], errors='coerce').replace(0, np.nan)

cleaner_tonum(clean_disparities,'disparities_White')
cleaner_tonum(clean_disparities,'disparities_Black')
cleaner_tonum(clean_disparities,'disparities_Asian/Pacific Islander')
cleaner_tonum(clean_disparities,'disparities_Hispanic')
cleaner_tonum(clean_disparities,'disparities_American Indian/Alaska Native')
cleaner_tonum(clean_disparities,'disparities_Other')


clean_disparities_tot = clean_disparities.pivot_table(index=["nhit_id_zip", "disparities_Urban/Rural"], 
                    columns='disparities_Condition', 
                    values=['disparities_Total']).reset_index()
clean_disparities_tot.columns = [f'{i}{j}' for i, j in clean_disparities_tot.columns]


clean_disparities_white = clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_White']).reset_index()
clean_disparities_white.columns = [f'{i}{j}' for i, j in clean_disparities_white.columns]


clean_disparities_black= clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_Black']).reset_index()
clean_disparities_black.columns = [f'{i}{j}' for i, j in clean_disparities_black.columns]


clean_disparities_asian= clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_Asian/Pacific Islander']).reset_index()
clean_disparities_asian.columns = [f'{i}{j}' for i, j in clean_disparities_asian.columns]


clean_disparities_hispanic= clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_Hispanic']).reset_index()
clean_disparities_hispanic.columns = [f'{i}{j}' for i, j in clean_disparities_hispanic.columns]


clean_disparities_americanindian= clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_American Indian/Alaska Native']).reset_index()
clean_disparities_americanindian.columns = [f'{i}{j}' for i, j in clean_disparities_americanindian.columns]

clean_disparities_other= clean_disparities.pivot_table(index=["nhit_id_zip"], 
                    columns='disparities_Condition', 
                    values=['disparities_Other']).reset_index()
clean_disparities_other.columns = [f'{i}{j}' for i, j in clean_disparities_other.columns]


clean_disparities = clean_disparities_tot.merge(clean_disparities_white, how='left', on='nhit_id_zip')
clean_disparities = clean_disparities.merge(clean_disparities_black, how='left', on='nhit_id_zip')
clean_disparities = clean_disparities.merge(clean_disparities_asian, how='left', on='nhit_id_zip')
clean_disparities = clean_disparities.merge(clean_disparities_hispanic, how='left', on='nhit_id_zip')
clean_disparities = clean_disparities.merge(clean_disparities_americanindian, how='left', on='nhit_id_zip')
clean_disparities = clean_disparities.merge(clean_disparities_other, how='left', on='nhit_id_zip')


del(clean_disparities_tot, clean_disparities_white, clean_disparities_black, clean_disparities_asian,
    clean_disparities_hispanic, clean_disparities_americanindian, clean_disparities_other)






#VULNERABILITY
# For list of variables: https://data.cdc.gov/Health-Statistics/Social-Vulnerability-Index-2018-United-States-coun/48va-t53r
original_vulnerability = pd.read_json("https://nhit-odp.data.socrata.com/resource/ippk-x3af.json?$limit=50000")
clean_vulnerability = original_vulnerability.drop(columns=['shape'])
clean_vulnerability = clean_vulnerability.add_prefix('vulnerability_')
clean_vulnerability['vulnerability_county'] = clean_vulnerability['vulnerability_county'].str.upper() 
clean_vulnerability['nhit_id_zip'] = clean_vulnerability['vulnerability_fips']
clean_vulnerability = clean_vulnerability.drop(columns=['vulnerability_state', 
                                                        'vulnerability_fips', 'vulnerability_location',
                                                        'vulnerability_st'])





#INTERNET ACCESS
original_internet = pd.read_json("https://nhit-odp.data.socrata.com/resource/bbux-k9wu.json?$limit=50000")
clean_internet = original_internet.add_prefix('internet_')
clean_internet['internet_name'] = clean_internet['internet_name'].str.encode('ascii', 'ignore').str.decode('ascii') #this line is important for saving our version to sql 
clean_internet['nhit_id_zip'] = [x.split('0500000US')[1] for x in clean_internet['internet_geo_id']]
clean_internet['nhit_id_zip'] = clean_internet['nhit_id_zip'].astype(int)
clean_internet = clean_internet.drop(columns=['internet_name',
                                             'internet_geo_id',
                                             'internet_geo_type',
                                             'internet_date',
                                             'internet_variable_code',
                                             'internet_internal_point',
                                             'internet_:@computed_region_cnz6_8gps',
                                             'internet_:@computed_region_9bv7_e982'])


clean_internet = clean_internet.pivot_table(index=["nhit_id_zip"], 
                    columns=['internet_year', 'internet_variable_label'], 
                    values='internet_value').reset_index()

clean_internet.columns = [f'{i}{j}' for i, j in clean_internet.columns]








#MERGE TOGETHER index 
clean_covid_vulnerability= clean_covid.merge(clean_vulnerability, how='outer', on='nhit_id_zip')
clean_covid_vulnerability_internet = clean_covid_vulnerability.merge(clean_internet, how='outer', on='nhit_id_zip')
clean_covid_vulnerability_internet_disparities= clean_covid_vulnerability_internet.merge(clean_disparities, how='outer', on='nhit_id_zip')




##NOW BRING IN LAT AND LONG 
final = clean_covid_vulnerability_internet_disparities.merge(zip_latlong, how='left', left_on='nhit_id_zip', right_on='gpsdata_Zip')

##SAVE LOCALLY 
final.to_csv('/Users/hantswilliams/Downloads/morehouse_demo.csv')






####SAVE TO DB FOR BACKUP 
MYSQL_HOSTNAME = '34.204.170.21'
MYSQL_USER = 'dba'
MYSQL_PASSWORD = 'ahi2020'
MYSQL_DATABASE = 'nhit'

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'
engine = create_engine(connection_string)

merge3.to_sql("morehouse_demo", con=engine, if_exists='replace')











