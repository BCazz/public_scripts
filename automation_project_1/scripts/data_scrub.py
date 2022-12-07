import pandas as pd 
import numpy as np
import os as os
import datetime as dt
import psycopg2 as pg2


def data_scrub():

	print('scrubbing data...')

	# creating data frames for each of the excel files

	inputs_folder_path = 'Q:\\Option Model\\GitHub\\advanced_power_repo\\margin_reports\\inputs\\'

	master_sheet = 'master_sheet.xlsx'
	tenaska_file = 'RT Energy Imbalance.xlsx'

	master_sheet = os.path.join(inputs_folder_path, master_sheet)
	tenaska_file = os.path.join(inputs_folder_path, tenaska_file)

	df_revenue_data = pd.read_excel(tenaska_file, sheet_name = 'Imbalance chart')
	df_generation_data = pd.read_excel(tenaska_file, sheet_name = 'Settlement Volumes Chart')
	df_max_generation_data = pd.read_excel(tenaska_file, sheet_name = 'Settlement Volumes Chart')
	df_power_data = pd.read_excel(master_sheet, sheet_name = 'ercot_raw')

	# creating a clean data frame for the revenue data

	df_revenue_data = df_revenue_data.loc[:, ['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','Amount - Credit Positive']]

	# dropping any instance of tenaska summing the data

	if df_revenue_data.iloc[0,0] == '(All)':
		df_revenue_data = df_revenue_data.drop(0)

	df_revenue_data['date'] = pd.to_datetime(df_revenue_data['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format = '%Y-%m-%d %X', utc = False)
	df_revenue_data['date'] = df_revenue_data['date'].dt.strftime('%Y-%m-%d')
	df_revenue_data = df_revenue_data.groupby(['date']).sum()

	# creating a clean data frame for the generation data

	df_generation_data = df_generation_data.loc[:, ['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','ENT_Telemetered_Output']]
	
	# dropping any instance of tenaska summing the data

	if df_generation_data.iloc[0,0] == '(All)':
		df_generation_data = df_generation_data.drop(0)

	df_generation_data['date'] = pd.to_datetime(df_generation_data['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format = '%Y-%m-%d %X', utc = False)
	df_generation_data['date'] = df_generation_data['date'].dt.strftime('%Y-%m-%d')
	df_generation_data = df_generation_data.groupby(['date']).sum()

	# creating a clean data frame for the day's max generation data

	df_max_generation_data = df_max_generation_data.loc[:, ['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','ENT_Telemetered_Output']]

	# dropping any instance of tenaska summing the data

	if df_max_generation_data.iloc[0,0] == '(All)':
		df_max_generation_data = df_max_generation_data.drop(0)

	df_max_generation_data['date'] = pd.to_datetime(df_max_generation_data['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format = '%Y-%m-%d %X', utc = False)
	df_max_generation_data['date'] = df_max_generation_data['date'].dt.strftime('%Y-%m-%d')
	df_max_generation_data['gen_max'] = df_max_generation_data.groupby('date')['ENT_Telemetered_Output'].transform('max')
	df_max_generation_data = df_max_generation_data.groupby(['date']).mean()

	# filtering df_power_data for the relevant hub and node pricing data 

	df_houston_hub = df_power_data.loc[df_power_data.location == 'HB_HOUSTON', ['date','timestamp','rt_lmp']]
	df_rowland = df_power_data.loc[df_power_data.location == 'ROW_ALL', ['date','timestamp','rt_lmp']]

	# clearing date issue

	df_houston_hub['time'] = pd.to_datetime(df_houston_hub['timestamp'], format = '%Y-%m-%dT%H:%M:%S.%f%z', utc = True)
	df_houston_hub['time'] = df_houston_hub['time'].dt.tz_convert('US/Eastern')
	df_houston_hub['time'] = df_houston_hub['time'].dt.tz_localize(None)
	df_houston_hub['time'] = df_houston_hub['time'].dt.strftime('%Y-%m-%d %X')

	df_houston_hub['date'] = pd.to_datetime(df_houston_hub['date'])

	# creating a data frame of the clean data aggregated to be pulled from later

	df_power_hourly = pd.DataFrame()

	df_power_hourly['date'] = df_houston_hub['date'].values
	df_power_hourly['time'] = df_houston_hub['time'].values
	df_power_hourly['houston_hub_lmp'] = df_houston_hub.loc[:,'rt_lmp'].values
	df_power_hourly['rowland_lmp'] = df_rowland['rt_lmp'].values
	df_power_hourly['rowland_basis'] = df_power_hourly['rowland_lmp'] - df_power_hourly['houston_hub_lmp']

	df_power_min = df_power_hourly
	df_power_min['rowland_lmp_min'] = df_power_hourly.groupby('date')['rowland_lmp'].transform('min')
	df_power_min = df_power_min.groupby(['date']).mean()

	df_power_max = df_power_hourly
	df_power_max['rowland_lmp_max'] = df_power_hourly.groupby('date')['rowland_lmp'].transform('max') 
	df_power_max = df_power_max.groupby(['date']).mean()

	# creating master dataframes that are database ready

	df_cutlass_margin = pd.DataFrame()

	df_cutlass_margin['date'] = pd.Series(df_power_hourly.loc[::24,'date'].values)
	df_cutlass_margin['gen_revenue'] = pd.Series(df_revenue_data['Amount - Credit Positive'].values) 
	df_cutlass_margin['gen_mw'] = pd.Series(df_generation_data['ENT_Telemetered_Output'].values)
	df_cutlass_margin['gen_mw_max'] = pd.Series(df_max_generation_data['gen_max'].values)
	df_cutlass_margin['rowland_lmp_min'] = pd.Series(df_power_min['rowland_lmp_min'].values)
	df_cutlass_margin['rowland_lmp_realized'] = pd.Series(df_cutlass_margin['gen_revenue'].values) / pd.Series(df_cutlass_margin['gen_mw'].values)
	df_cutlass_margin['rowland_lmp_max'] = pd.Series(df_power_max['rowland_lmp_max'].values)
	df_cutlass_margin['rowland_basis'] = pd.Series(df_power_hourly.groupby(['date'])['rowland_basis'].mean().values)

	# exporting data frames to a csv files that can be imported into postgres sql

	df_cutlass_margin.to_csv('Q:\\Option Model\\GitHub\\advanced_power_repo\\margin_reports\\outputs\\cutlass_margin_temp.csv', index = False, encoding='utf-8')

	# connecting to the postgres database & creating a cursor

	conn = pg2.connect(database='ap_am_db',user='postgres',password='Bcazz1', host = '10.113.30.149')
	cur = conn.cursor()

	# drop any previous existence of the temporary table that will be used to upsert the margin report data into the database

	cur.execute('DROP TABLE IF EXISTS z_cutlass_margin_temp;')
	
	# create a temporary sql table that will be used to upsert the data in df_cce_margin_db into the primary database

	cur.execute("""CREATE TABLE z_cutlass_margin_temp(
		date TIMESTAMP,
		gen_revenue NUMERIC,
		gen_mw NUMERIC,
		gen_mw_max NUMERIC,
		rowland_lmp_min NUMERIC,
		rowland_lmp_realized NUMERIC,
		rowland_lmp_max NUMERIC,
		rowland_basis NUMERIC,
		PRIMARY KEY (date)
	);""")

	# uploading df_cutlass_margin to the z_cutlass_margin_temp table in the database

	cutlass_margin_temp_file = open('Q:\\Option Model\\GitHub\\advanced_power_repo\\margin_reports\\outputs\\cutlass_margin_temp.csv')

	import_margin = """
	COPY z_cutlass_margin_temp FROM STDIN WITH
		CSV
		HEADER
		DELIMITER AS ','
	"""
	cur.copy_expert(sql=import_margin, file=cutlass_margin_temp_file)

	# upsert cutlass_margin_temp into cutlass_margin

	upsert_margin = """
		INSERT INTO cutlass_margin(date,gen_revenue,gen_mw,gen_mw_max,rowland_lmp_min,rowland_lmp_realized,rowland_lmp_max,rowland_basis)
  			SELECT z_cutlass_margin_temp.date,
         	       z_cutlass_margin_temp.gen_revenue,
        	       z_cutlass_margin_temp.gen_mw,
        	       z_cutlass_margin_temp.gen_mw_max,
        	       z_cutlass_margin_temp.rowland_lmp_min,
        	       z_cutlass_margin_temp.rowland_lmp_realized,
        	       z_cutlass_margin_temp.rowland_lmp_max,
        	       z_cutlass_margin_temp.rowland_basis
        
			FROM ( SELECT
         	       z_cutlass_margin_temp.date,
         	       z_cutlass_margin_temp.gen_revenue,
        	       z_cutlass_margin_temp.gen_mw,
        	       z_cutlass_margin_temp.gen_mw_max,
        	       z_cutlass_margin_temp.rowland_lmp_min,
        	       z_cutlass_margin_temp.rowland_lmp_realized,
        	       z_cutlass_margin_temp.rowland_lmp_max,
        	       z_cutlass_margin_temp.rowland_basis
			       FROM z_cutlass_margin_temp
			 ) z_cutlass_margin_temp
		
			ON CONFLICT(date)
		
			DO UPDATE SET
			        gen_revenue = EXCLUDED.gen_revenue,
			        gen_mw = EXCLUDED.gen_mw,
			        gen_mw_max = EXCLUDED.gen_mw_max,
			        rowland_lmp_min = EXCLUDED.rowland_lmp_min,
			        rowland_lmp_realized = EXCLUDED.rowland_lmp_realized,
			        rowland_lmp_max = EXCLUDED.rowland_lmp_max,
			        rowland_basis = EXCLUDED.rowland_basis;
	"""

	cur.execute(upsert_margin)

	cur.execute('GRANT SELECT ON TABLE cutlass_margin TO PUBLIC')
	conn.commit()

	# closing connection to the database

	cur = conn.close()

	print('cutlass margin and settlements data scrubbed')

#cutlass_margin_report()