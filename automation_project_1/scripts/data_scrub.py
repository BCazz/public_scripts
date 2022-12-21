import requests
import json
import pandas as pd
import datetime as dt
import psycopg2 as pg2


print('processing...')

data_file = 'C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1\\inputs\\data.xlsx'

# https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
# https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

print('reading excel file...')

df_revenue = pd.read_excel(data_file, sheet_name='Imbalance chart')
df_generation = pd.read_excel(data_file, sheet_name='Settlement Volumes Chart')

# https://requests.readthedocs.io/en/latest/
# https://requests.readthedocs.io/en/latest/user/quickstart/#make-a-request
# https://requests.readthedocs.io/en/latest/user/authentication/

# https://www.youtube.com/watch?v=tb8gHvYlCFs
# https://www.youtube.com/watch?v=fklHBWow8vE&t=1349s

print('pulling api data...')

start_date = '2022-12-01'
end_date = '2023-02-01'

url = f'https://ams.pharos-ei.com/api/ercot/lmp/historic/hourly?start_date={start_date}&end_date={end_date}&location_id=ROW_ALL,HB_HOUSTON'
key = '6308505addb030a907bd1ZxSfq2W7wY6xfBHVspqM'

df_power = requests.get(url,auth=(key,'*')).text
df_power = json.loads(df_power)
df_power = df_power['lmp']
df_power = pd.DataFrame(df_power)

# cleaning up revenue data
# pulling out only desired columns

print('cleaning revenue data...')

df_revenue = df_revenue.loc[:,['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','Amount - Credit Positive']]

if df_revenue.iloc[0,0] =='(All)':
	df_revenue = df_revenue.drop(0)
else:
	pass

# https://dataindependent.com/pandas/pandas-to-datetime-string-to-date-pd-to_datetime/

df_revenue['date'] = pd.to_datetime(df_revenue['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format='%Y-%m-%d %X')
df_revenue['date'] = df_revenue['date'].dt.strftime('%Y-%m-%d')
df_revenue = df_revenue.groupby('date', as_index=False).sum()

print('cleaning generation data...')

# cleaning up generation data

df_generation = df_generation.loc[:,['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','ENT_Telemetered_Output']]

if df_generation.iloc[0,0] =='(All)':
	df_generation = df_generation.drop(0)
else:
	pass

df_generation['date'] = pd.to_datetime(df_generation['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format='%Y-%m-%d %X')
df_generation['date'] = df_generation['date'].dt.strftime('%Y-%m-%d')

df_generation_max = df_generation
df_generation_max['gen_max'] = df_generation_max.groupby('date')['ENT_Telemetered_Output'].transform('max')
df_generation_max = df_generation_max.groupby('date', as_index=False).mean()

df_generation = df_generation.groupby('date', as_index=False).sum()

print('cleaning power data...')

# cleaning up power data

df_houston = df_power.loc[df_power.location == 'HB_HOUSTON',['date','timestamp','rt_lmp']]	
df_row = df_power.loc[df_power.location == 'ROW_ALL',['date','timestamp','rt_lmp']]

df_houston['time'] = pd.to_datetime(df_houston['timestamp'], format='%Y-%m-%dT%X.%f%z', utc = True)
df_houston['time'] = df_houston['time'].dt.tz_convert('US/Central')
df_houston['time'] = df_houston['time'].dt.tz_localize(None)
df_houston['time'] = df_houston['time'].dt.strftime('%Y-%m-%d %X')

df_houston['date'] = pd.to_datetime(df_houston['date'])

# creating a new dataframe to have all the hourly power data desired

df_power_hourly = pd.DataFrame()

df_power_hourly['date'] = df_houston['date'].values
df_power_hourly['time'] = df_houston['time'].values
df_power_hourly['houston_hub_lmp'] = df_houston['rt_lmp'].values
df_power_hourly['rowland_lmp'] = df_row['rt_lmp'].values
df_power_hourly['rowland_basis'] = df_power_hourly['rowland_lmp'] - df_power_hourly['houston_hub_lmp']

df_power_min = df_power_hourly
df_power_min['rowland_lmp_min'] = df_power_min.groupby('date')['rowland_lmp'].transform('min')
df_power_min = df_power_min.groupby('date').mean()

df_power_max = df_power_hourly
df_power_max['rowland_lmp_max'] = df_power_max.groupby('date')['rowland_lmp'].transform('max')
df_power_max = df_power_max.groupby('date').mean()

print('compiling data into a master dataframe...')

# creating master dataframes that are database ready

df_master = pd.DataFrame()

df_master['date'] = pd.Series(df_revenue['date'].values)
df_master['gen_revenue'] =  pd.Series(df_revenue['Amount - Credit Positive'].values)
df_master['gen_mw'] = pd.Series(df_generation['ENT_Telemetered_Output'].values)
df_master['gen_mw_max'] = pd.Series(df_generation_max['gen_max'].values)
df_master['lmp_min'] = pd.Series(df_power_min['rowland_lmp_min'].values)
df_master['lmp_realized'] = pd.Series(df_master['gen_revenue'] / df_master['gen_mw'].values)
df_master['lmp_max'] = pd.Series(df_power_max['rowland_lmp_max'].values)
df_master['basis'] = pd.Series(df_power_hourly.groupby('date')['rowland_basis'].mean().values)

# exporting the df_master dataframe to a csv that be upserted into SQL

print('exporting to csv...')

# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

df_master.to_csv('C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1\\outputs\\final_data.csv', index = False, encoding = 'utf-8')

# https://www.psycopg.org/docs/

print('connecting to database...')

# create a connection and cursor

conn = pg2.connect(database='automation_projects',user='postgres',password='XXXX',host = '127.0.0.1')
cur = conn.cursor()

# drop any existence of the temporary table used to upset data into the database

cur.execute('DROP TABLE IF EXISTS z_auto_project_1;')

# create a temporary sql table that will be used to upsert the data into the primary table

cur.execute("""CREATE TABLE z_auto_project_1(
	date TIMESTAMP,
	gen_revenue NUMERIC,
	gen_mw NUMERIC,
	gen_mw_max NUMERIC,
	lmp_min NUMERIC,
	lmp_realized NUMERIC,
	lmp_max NUMERIC,
	basis NUMERIC,
	PRIMARY KEY (date)
);""")

# upload the csv to the z_auto.. table in the database

project_1_file = open('C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1\\outputs\\final_data.csv')

import_data = """
COPY z_auto_project_1 FROM STDIN WITH
	CSV
	HEADER
	DELIMITER AS ','
"""

cur.copy_expert(sql=import_data,file=project_1_file)

print('upserting data...')

# upserting temp table into the primary table

upsert = """
	INSERT INTO auto_project_1(date,gen_revenue,gen_mw,gen_mw_max,lmp_min,lmp_realized,lmp_max,basis)
		SELECT  z_auto_project_1.date,
				z_auto_project_1.gen_revenue,
				z_auto_project_1.gen_mw,
				z_auto_project_1.gen_mw_max,
				z_auto_project_1.lmp_min,
				z_auto_project_1.lmp_realized,
				z_auto_project_1.lmp_max,
				z_auto_project_1.basis

		FROM ( SELECT
				z_auto_project_1.date,
				z_auto_project_1.gen_revenue,
				z_auto_project_1.gen_mw,
				z_auto_project_1.gen_mw_max,
				z_auto_project_1.lmp_min,
				z_auto_project_1.lmp_realized,
				z_auto_project_1.lmp_max,
				z_auto_project_1.basis
				FROM z_auto_project_1
		) z_auto_project_1

		ON CONFLICT (date)

		DO UPDATE SET
			gen_revenue = EXCLUDED.gen_revenue,
			gen_mw = EXCLUDED.gen_mw,
			gen_mw_max = EXCLUDED.gen_mw_max,
			lmp_min = EXCLUDED.lmp_min,
			lmp_realized = EXCLUDED.lmp_realized,
			lmp_max = EXCLUDED.lmp_max,
			basis = EXCLUDED.basis;
"""

cur.execute(upsert)

# committing the sql statements above

conn.commit()

print('program complete -- all data scrubbed and exported to database')