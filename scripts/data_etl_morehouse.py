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


clean_disparities_tot = clean_disparities.pivot_table(index=["disparities_FIPS", "disparities_Urban/Rural"], 
                    columns='disparities_Condition', 
                    values=['disparities_Total']).reset_index()
clean_disparities_tot.columns = [f'{i}{j}' for i, j in clean_disparities_tot.columns]


clean_disparities_white = clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_White']).reset_index()
clean_disparities_white.columns = [f'{i}{j}' for i, j in clean_disparities_white.columns]


clean_disparities_black= clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_Black']).reset_index()
clean_disparities_black.columns = [f'{i}{j}' for i, j in clean_disparities_black.columns]


clean_disparities_asian= clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_Asian/Pacific Islander']).reset_index()
clean_disparities_asian.columns = [f'{i}{j}' for i, j in clean_disparities_asian.columns]


clean_disparities_hispanic= clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_Hispanic']).reset_index()
clean_disparities_hispanic.columns = [f'{i}{j}' for i, j in clean_disparities_hispanic.columns]


clean_disparities_americanindian= clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_American Indian/Alaska Native']).reset_index()
clean_disparities_americanindian.columns = [f'{i}{j}' for i, j in clean_disparities_americanindian.columns]

clean_disparities_other= clean_disparities.pivot_table(index=["disparities_FIPS"], 
                    columns='disparities_Condition', 
                    values=['disparities_Other']).reset_index()
clean_disparities_other.columns = [f'{i}{j}' for i, j in clean_disparities_other.columns]




clean_disparities = clean_disparities_tot.merge(clean_disparities_white, how='left', on='disparities_FIPS')
clean_disparities = clean_disparities.merge(clean_disparities_black, how='left', on='disparities_FIPS')
clean_disparities = clean_disparities.merge(clean_disparities_asian, how='left', on='disparities_FIPS')
clean_disparities = clean_disparities.merge(clean_disparities_hispanic, how='left', on='disparities_FIPS')
clean_disparities = clean_disparities.merge(clean_disparities_americanindian, how='left', on='disparities_FIPS')
clean_disparities = clean_disparities.merge(clean_disparities_other, how='left', on='disparities_FIPS')


del(clean_disparities_tot, clean_disparities_white, clean_disparities_black, clean_disparities_asian,
    clean_disparities_hispanic, clean_disparities_americanindian, clean_disparities_other)












#VULNERABILITY
# For list of variables: https://data.cdc.gov/Health-Statistics/Social-Vulnerability-Index-2018-United-States-coun/48va-t53r
original_vulnerability = pd.read_json("https://nhit-odp.data.socrata.com/resource/ippk-x3af.json?$limit=50000")
clean_vulnerability = original_vulnerability.drop(columns=['shape'])
clean_vulnerability = clean_vulnerability.add_prefix('vulnerability_')
clean_vulnerability['vulnerability_county'] = clean_vulnerability['vulnerability_county'].str.upper() 










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





clean_internet = clean_internet.pivot_table(index=["fips_internet_geo_id", "internet_name", "internet_Latitude", "internet_Longitude"], 
                    columns=['internet_year', 'internet_variable_label'], 
                    values='internet_value').reset_index()

clean_internet.columns = [f'{i}{j}' for i, j in clean_internet.columns]








#MERGE TOGETHER index 
merge1 = clean_internet.merge(clean_disparities, how='outer', left_on='fips_internet_geo_id', right_on='disparities_FIPS')
merge2 = merge1.merge(clean_vulnerability, how='outer', left_on='fips_internet_geo_id', right_on='vulnerability_fips')
merge3 = merge2.merge(clean_covid, how='left', left_on='fips_internet_geo_id', right_on='covid_fips_county_code')


merge3_covid = clean_covid.merge(clean_internet, how='left', left_on='covid_fips_county_code', right_on='fips_internet_geo_id')
merge3_covid = merge3_covid.merge(clean_disparities, how='left', left_on='covid_fips_county_code', right_on='disparities_FIPS')
merge3_covid = merge3_covid.merge(clean_vulnerability, how='left', left_on='covid_fips_county_code', right_on='vulnerability_fips')
merge3_covid = merge3_covid.drop(columns=['covid_last_week','fips_internet_geo_id','internet_Latitude', 'internet_Longitude',
                                         'disparities_FIPS', 'vulnerability_st', 'vulnerability_fips', 'vulnerability_st',
                                             'vulnerability_state', 'covid_county_name',
                                             'vulnerability_st_abbr',])
merge3_covid = merge3_covid[[
         'covid_Latitude',
         'covid_Longitude',
         'covid_state',
         'covid_fips_county_code',
         'vulnerability_county',
         'vulnerability_location',
         'covid_date_as_of',
         'covid_deaths_involving_covid_19',
         'covid_deaths_from_all_causes',
         '2017Households with no internet access',
         '2017Population below poverty level',
         '2017Population over 60 years old',
         '2018Households with no internet access',
         '2018Population below poverty level',
         '2018Population over 60 years old',
         'disparities_Urban/Rural',
         'disparities_TotalChronic Kidney Disease',
         'disparities_TotalChronic Obstructive Pulmonary Disease',
         'disparities_TotalCongestive Heart Failure',
         'disparities_TotalDiabetes',
         'disparities_TotalHypertension',
         'disparities_WhiteChronic Kidney Disease',
         'disparities_WhiteChronic Obstructive Pulmonary Disease',
         'disparities_WhiteCongestive Heart Failure',
         'disparities_WhiteDiabetes',
         'disparities_WhiteHypertension',
         'disparities_BlackChronic Kidney Disease',
         'disparities_BlackChronic Obstructive Pulmonary Disease',
         'disparities_BlackCongestive Heart Failure',
         'disparities_BlackDiabetes',
         'disparities_BlackHypertension',
         'disparities_Asian/Pacific IslanderChronic Kidney Disease',
         'disparities_Asian/Pacific IslanderChronic Obstructive Pulmonary Disease',
         'disparities_Asian/Pacific IslanderCongestive Heart Failure',
         'disparities_Asian/Pacific IslanderDiabetes',
         'disparities_Asian/Pacific IslanderHypertension',
         'disparities_HispanicChronic Kidney Disease',
         'disparities_HispanicChronic Obstructive Pulmonary Disease',
         'disparities_HispanicCongestive Heart Failure',
         'disparities_HispanicDiabetes',
         'disparities_HispanicHypertension',
         'disparities_American Indian/Alaska NativeChronic Kidney Disease',
         'disparities_American Indian/Alaska NativeChronic Obstructive Pulmonary Disease',
         'disparities_American Indian/Alaska NativeCongestive Heart Failure',
         'disparities_American Indian/Alaska NativeDiabetes',
         'disparities_American Indian/Alaska NativeHypertension',
         'disparities_OtherChronic Kidney Disease',
         'disparities_OtherChronic Obstructive Pulmonary Disease',
         'disparities_OtherCongestive Heart Failure',
         'disparities_OtherDiabetes',
         'disparities_OtherHypertension',
         'vulnerability_area_sqmi',
         'vulnerability_e_totpop',
         'vulnerability_m_totpop',
         'vulnerability_e_hu',
         'vulnerability_m_hu',
         'vulnerability_e_hh',
         'vulnerability_m_hh',
         'vulnerability_e_pov',
         'vulnerability_m_pov',
         'vulnerability_e_unemp',
         'vulnerability_m_unemp',
         'vulnerability_e_pci',
         'vulnerability_m_pci',
         'vulnerability_e_nohsdp',
         'vulnerability_m_nohsdp',
         'vulnerability_e_age65',
         'vulnerability_m_age65',
         'vulnerability_e_age17',
         'vulnerability_m_age17',
         'vulnerability_e_disabl',
         'vulnerability_m_disabl',
         'vulnerability_e_sngpnt',
         'vulnerability_m_sngpnt',
         'vulnerability_e_minrty',
         'vulnerability_m_minrty',
         'vulnerability_e_limeng',
         'vulnerability_m_limeng',
         'vulnerability_e_munit',
         'vulnerability_m_munit',
         'vulnerability_e_mobile',
         'vulnerability_m_mobile',
         'vulnerability_e_crowd',
         'vulnerability_m_crowd',
         'vulnerability_e_noveh',
         'vulnerability_m_noveh',
         'vulnerability_e_groupq',
         'vulnerability_m_groupq',
         'vulnerability_ep_pov',
         'vulnerability_mp_pov',
         'vulnerability_ep_unemp',
         'vulnerability_mp_unemp',
         'vulnerability_ep_pci',
         'vulnerability_mp_pci',
         'vulnerability_ep_nohsdp',
         'vulnerability_mp_nohsdp',
         'vulnerability_ep_age65',
         'vulnerability_mp_age65',
         'vulnerability_ep_age17',
         'vulnerability_mp_age17',
         'vulnerability_ep_disabl',
         'vulnerability_mp_disabl',
         'vulnerability_ep_sngpnt',
         'vulnerability_mp_sngpnt',
         'vulnerability_ep_minrty',
         'vulnerability_mp_minrty',
         'vulnerability_ep_limeng',
         'vulnerability_mp_limeng',
         'vulnerability_ep_munit',
         'vulnerability_mp_munit',
         'vulnerability_ep_mobile',
         'vulnerability_mp_mobile',
         'vulnerability_ep_crowd',
         'vulnerability_mp_crowd',
         'vulnerability_ep_noveh',
         'vulnerability_mp_noveh',
         'vulnerability_ep_groupq',
         'vulnerability_mp_groupq',
         'vulnerability_epl_pov',
         'vulnerability_epl_unemp',
         'vulnerability_epl_pci',
         'vulnerability_epl_nohsdp',
         'vulnerability_spl_theme1',
         'vulnerability_rpl_theme1',
         'vulnerability_epl_age65',
         'vulnerability_epl_age17',
         'vulnerability_epl_disabl',
         'vulnerability_epl_sngpnt',
         'vulnerability_spl_theme2',
         'vulnerability_rpl_theme2',
         'vulnerability_epl_minrty',
         'vulnerability_epl_limeng',
         'vulnerability_spl_theme3',
         'vulnerability_rpl_theme3',
         'vulnerability_epl_munit',
         'vulnerability_epl_mobile',
         'vulnerability_epl_crowd',
         'vulnerability_epl_noveh',
         'vulnerability_epl_groupq',
         'vulnerability_spl_theme4',
         'vulnerability_rpl_theme4',
         'vulnerability_spl_themes',
         'vulnerability_rpl_themes',
         'vulnerability_f_pov',
         'vulnerability_f_unemp',
         'vulnerability_f_pci',
         'vulnerability_f_nohsdp',
         'vulnerability_f_theme1',
         'vulnerability_f_age65',
         'vulnerability_f_age17',
         'vulnerability_f_disabl',
         'vulnerability_f_sngpnt',
         'vulnerability_f_theme2',
         'vulnerability_f_minrty',
         'vulnerability_f_limeng',
         'vulnerability_f_theme3',
         'vulnerability_f_munit',
         'vulnerability_f_mobile',
         'vulnerability_f_crowd',
         'vulnerability_f_noveh',
         'vulnerability_f_groupq',
         'vulnerability_f_theme4',
         'vulnerability_f_total',
         'vulnerability_e_uninsur',
         'vulnerability_m_uninsur',
         'vulnerability_ep_uninsur',
         'vulnerability_mp_uninsur',
         'vulnerability_e_daypop'
    ]]



merge3_covid = merge3_covid.rename(columns={
    'covid_Latitude': 'Latitude', 
    'covid_Longitude': 'Longitude',
    'covid_state': 'State',
    'covid_fips_county_code': 'Fips_County_Code',
    'vulnerability_county': 'County',
    'vulnerability_location': 'County_State',
    })




#BRING IN CHSI Data (Health Indicators)
chsi = pd.read_csv("https://raw.githubusercontent.com/hantswilliams/AHI_NHIT_Dashboard/main/scripts/script_output/morehouse_chsi.csv")
chsi = chsi.drop(columns=['Unnamed: 0'])
chsi['chsi_CHSI_County_Name'] = chsi['chsi_CHSI_County_Name'].str.upper() 
chsi = chsi.rename(columns={
    'chsi_CHSI_County_Name': 'County',
    'chsi_CHSI_State_Abbr': 'State',
    })



final = merge3_covid.merge(chsi, how='left', on=['County', 'State'])


sample = final.sample(10)


##SAVE LOCALLY 
final.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/scripts/script_output/morehouse_demo.csv')







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

final.to_sql("morehouse_demo", con=engine, if_exists='replace')











