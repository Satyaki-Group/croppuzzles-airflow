import os
import io
import pandas as pd
from datetime import datetime

# Correct file paths - sample_data is in airflow_app folder, not dags
file1 = "../sample_data/202403_psd_alldata.csv"
file2 = "../sample_data/202403_psd_grains_pulses.csv"
file3 = "../sample_data/202403_psd_oilseeds.csv"

df=pd.read_csv(file1)
#print(df[df['Country_Code'].isnull()].head(10))
#print(df.duplicated().sum())
# give correct datatypes to columns
df = df.astype({
    'Commodity_Code': 'Int64',
    'Commodity_Description': 'string',
    'Country_Code': 'string',
    'Country_Name': 'string',
    'Market_Year': 'Int64',
    'Calendar_Year': 'Int64',
    'Month': 'Int64',
    'Attribute_ID': 'Int64',
    'Attribute_Description': 'string',
    'Unit_ID': 'Int64',
    'Unit_Description': 'string',
    'Value': 'float64'
})
# want to check douplicate using the combination of columns that define a unique record, not the entire row (since Value can differ for duplicates)
#print(df.duplicated(subset=['Commodity_Code','Country_Code','Market_Year','Calendar_Year','Month','Attribute_ID']).sum())  # this columns combination contain 0 duplicates

# print(df.duplicated(subset=['Attribute_ID']).sum()) # this column contain 2001720 duplicates

# Fill Country_Code missing values
mapping = df.dropna(subset=['Country_Code']).drop_duplicates(subset=['Country_Name']).set_index('Country_Name')['Country_Code']
df['Country_Code'] = df['Country_Code'].fillna(df['Country_Name'].map(mapping))
# check if there is still missing values in Country_Code
#print(df['Country_Code'].isnull().sum())  # there is still 1052 missing values


#print(df['Country_Name'][df['Country_Code'].isnull()].head(10))  
print(df.loc[df['Country_Code'].isnull(), 'Country_Name'].unique())
df['Country_Code'] = df['Country_Code'].fillna(df['Country_Name'].map({'Netherlands Antilles': 'AN'}))

print(df['Country_Code'].isnull().sum())  