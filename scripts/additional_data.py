#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 20:43:27 2020

@author: hantswilliams

Community Health Status Indicators (CHSI) to Combat Obesity, Heart Disease and Cancer
https://catalog.data.gov/dataset/community-health-status-indicators-chsi-to-combat-obesity-heart-disease-and-cancer/resource/356ffbd2-efd6-4d95-8c2e-be842b3291b5

U.S. Department of Health & Human Services — Community Health Status Indicators (CHSI) to combat obesity, heart disease, and cancer are major components of the Community Health Data Initiative. This dataset...

https://healthdata.gov/dataset/community-health-status-indicators-chsi-combat-obesity-heart-disease-and-cancer


For data translation of files, see the 'DATAELEMENTDESCRIPTION.csv' file - contains data dictionary 


-9999	Indicate N.A. value from the source data for the Unemployed column on the VUNERABLEPOPSANDENVHEALTH page
-2222 or -2222.2 or -2	nda, no data available, see Data Notes document for details
-1111.1 or -1111 or -1	nrf, no report, see Data Notes document for details


"""

import pandas as pd 
import numpy as np

demograhpics = pd.read_csv("/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/additional_data/chsi_dataset/KEEP_DEMOGRAPHICS.csv")
pre_services = pd.read_csv("/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/additional_data/chsi_dataset/KEEP_PREVENTIVESERVICESUSE.csv")
risk_factors = pd.read_csv("/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/additional_data/chsi_dataset/KEEP_RISKFACTORSANDACCESSTOCARE.csv")
summary_measures = pd.read_csv("/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/additional_data/chsi_dataset/KEEP_SUMMARYMEASURESOFHEALTH.csv")
vul_pops = pd.read_csv("/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/additional_data/chsi_dataset/KEEP_VUNERABLEPOPSANDENVHEALTH.csv")


chsi = demograhpics.merge(pre_services, how='outer', on=['State_FIPS_Code', 'County_FIPS_Code'])
chsi = chsi.merge(risk_factors, how='outer', on=['State_FIPS_Code', 'County_FIPS_Code'])
chsi = chsi.merge(summary_measures, how='outer', on=['State_FIPS_Code', 'County_FIPS_Code'])
chsi = chsi.merge(vul_pops, how='outer', on=['State_FIPS_Code', 'County_FIPS_Code'])

chsi = chsi.replace(-9999, np.nan)
chsi = chsi.replace(-2222, np.nan)
chsi = chsi.replace(-2222.2, np.nan)
chsi = chsi.replace(-2, np.nan)
chsi = chsi.replace(-1111.1, np.nan)
chsi = chsi.replace(-1111, np.nan)
chsi = chsi.replace(-1, np.nan)

sample = chsi.sample(100)

chsi = chsi.drop(columns=[
     'CHSI_County_Name_y',
     'CHSI_State_Name_y',
     'CHSI_State_Abbr_y',
     'Strata_ID_Number_y',
      'CHSI_County_Name_x',
     'CHSI_State_Name_x',
     'CHSI_State_Abbr_x',
     'Strata_ID_Number_x',
      'CHSI_County_Name_y',
     'CHSI_State_Name_y',
     'CHSI_State_Abbr_y',
     'Strata_ID_Number_y',
    ])

chsi = chsi[[
     'State_FIPS_Code',
     'County_FIPS_Code',
     'Strata_Determining_Factors',
     'CHSI_County_Name',
     'CHSI_State_Name',
     'CHSI_State_Abbr',
     'Strata_ID_Number',
     'Number_Counties',
     'Population_Size',
     'Min_Population_Size',
     'Max_Population_Size',
     'Population_Density',
     'Min_Population_Density',
     'Max_Population_Density',
     'Poverty',
     'Min_Poverty',
     'Max_Poverty',
     'Age_19_Under',
     'Min_Age_19_Under',
     'Max_Age_19_Under',
     'Age_19_64',
     'Min_Age_19_64',
     'Max_Age_19_65',
     'Age_65_84',
     'Min_Age_65_84',
     'Max_Age_65_85',
     'Age_85_and_Over',
     'Min_Age_85_and_Over',
     'Max_Age_85_and_Over',
     'White',
     'Min_White',
     'Max_White',
     'Black',
     'Min_Black',
     'Max_Black',
     'Native_American',
     'Min_Native_American',
     'Max_Native_American',
     'Asian',
     'Min_Asian',
     'Max_Asian',
     'Hispanic',
     'Min_Hispanic',
     'Max_Hispanic',
     'FluB_Rpt',
     'FluB_Ind',
     'FluB_Exp',
     'HepA_Rpt',
     'HepA_Ind',
     'HepA_Exp',
     'HepB_Rpt',
     'HepB_Ind',
     'HepB_Exp',
     'Meas_Rpt',
     'Meas_Ind',
     'Meas_Exp',
     'Pert_Rpt',
     'Pert_Ind',
     'Pert_Exp',
     'CRS_Rpt',
     'CRS_Ind',
     'CRS_Exp',
     'Syphilis_Rpt',
     'Syphilis_Ind',
     'Syphilis_Exp',
     'ID_Time_Span',
     'Pap_Smear',
     'CI_Min_Pap_Smear',
     'CI_Max_Pap_Smear',
     'Mammogram',
     'CI_Min_Mammogram',
     'CI_Max_Mammogram',
     'Proctoscopy',
     'CI_Min_Proctoscopy',
     'CI_Max_Proctoscopy',
     'Pneumo_Vax',
     'CI_Min_Pneumo_Vax',
     'CI_Max_Pneumo_Vax',
     'Flu_Vac',
     'CI_Min_Flu_Vac',
     'CI_Max_Flu_Vac',
     'No_Exercise',
     'CI_Min_No_Exercise',
     'CI_Max_No_Exercise',
     'Few_Fruit_Veg',
     'CI_Min_Fruit_Veg',
     'CI_Max_Fruit_Veg',
     'Obesity',
     'CI_Min_Obesity',
     'CI_Max_Obesity',
     'High_Blood_Pres',
     'CI_Min_High_Blood_Pres',
     'CI_Max_High_Blood_Pres',
     'Smoker',
     'CI_Min_Smoker',
     'CI_Max_Smoker',
     'Diabetes',
     'CI_Min_Diabetes',
     'CI_Max_Diabetes',
     'Uninsured',
     'Elderly_Medicare',
     'Disabled_Medicare',
     'Prim_Care_Phys_Rate',
     'Dentist_Rate',
     'Community_Health_Center_Ind',
     'HPSA_Ind',
     'ALE',
     'Min_ALE',
     'Max_ALE',
     'US_ALE',
     'All_Death',
     'Min_All_Death',
     'Max_All_Death',
     'US_All_Death',
     'CI_Min_All_Death',
     'CI_Max_All_Death',
     'Health_Status',
     'Min_Health_Status',
     'Max_Health_Status',
     'US_Health_Status',
     'CI_Min_Health_Status',
     'CI_Max_Health_Status',
     'Unhealthy_Days',
     'Min_Unhealthy_Days',
     'Max_Unhealthy_Days',
     'US_Unhealthy_Days',
     'CI_Min_Unhealthy_Days',
     'CI_Max_Unhealthy_Days',
     'No_HS_Diploma',
     'Unemployed',
     'Sev_Work_Disabled',
     'Major_Depression',
     'Recent_Drug_Use',
     'Ecol_Rpt',
     'Ecol_Rpt_Ind',
     'Ecol_Exp',
     'Salm_Rpt',
     'Salm_Rpt_Ind',
     'Salm_Exp',
     'Shig_Rpt',
     'Shig_Rpt_Ind',
     'Shig_Exp',
     'Toxic_Chem',
     'Carbon_Monoxide_Ind',
     'Nitrogen_Dioxide_Ind',
     'Sulfur_Dioxide_Ind',
     'Ozone_Ind',
     'Particulate_Matter_Ind',
     'Lead_Ind',
     'EH_Time_Span'
    ]]



chsi = chsi.add_prefix('chsi_')

chsi.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/ahi/AHI_NHIT_Dashboard/scripts/script_output/morehouse_chsi.csv')





