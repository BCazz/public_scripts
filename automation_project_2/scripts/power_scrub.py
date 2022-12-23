import pandas as pd 
import os as os 
import glob as glob
from functools import reduce
import psycopg2 as pg2

print('scrubbing power data...')

# setting the working directory and looping through the power files

os.chdir("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs\\")

files = glob.glob('icecleared_power*')

for file in files:

	# reading the excel file as a dataframe

	power_data = pd.read_excel(file)

	# creating a function to pull out the desired on peak and off peak prices

	def data_scrub(hub,name_on,name_off):

		dataframe_on_peak = power_data.loc[power_data.HUB == f'{hub}', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]
		dataframe_on_peak = dataframe_on_peak.loc[dataframe_on_peak.PRODUCT == 'Peak Futures (1 MW)', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]
		dataframe_off_peak = power_data.loc[power_data.HUB == f'{hub} Off-Peak', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]
		dataframe_off_peak = dataframe_off_peak.loc[dataframe_off_peak.PRODUCT == 'Off-Peak Futures (1 MW)', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]

		dataframe_on_peak['strip'] = dataframe_on_peak['STRIP']
		dataframe_on_peak[name_on] = dataframe_on_peak['SETTLEMENT PRICE']  
		dataframe_off_peak['strip'] = dataframe_off_peak['STRIP']
		dataframe_off_peak[name_off] = dataframe_off_peak['SETTLEMENT PRICE']

		# documentation --> https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
		# visual demo --> https://www.freecodecamp.org/news/sql-join-types-inner-join-vs-outer-join-example/
		# video explanation --> https://www.youtube.com/watch?v=zGSv0VaOtR0
				
		dataframe_final = dataframe_on_peak.merge(dataframe_off_peak,how='outer',on='strip')
		dataframe_final = dataframe_final.loc[:,['strip',name_on,name_off]]

		return dataframe_final

	# using the function to get dataframes of the desired prices

	ad_hub = data_scrub('AD Hub DA','ad_hub_on_peak','ad_hub_off_peak')
	zone_g = data_scrub('NYISO G','zone_g_on_peak','zone_g_off_peak')
	zone_f = data_scrub('NYISO F','zone_f_on_peak','zone_f_off_peak')
	mass_hub = data_scrub('Nepool MH DA','mass_hub_on_peak','mass_hub_off_peak')
	houston_hub_rt = data_scrub('ERCOT Houston 345KV Hub RT','houston_hub_on_peak_rt','houston_hub_off_peak_rt')
	houston_hub_da = data_scrub('ERCOT Houston 345KV Hub DA','houston_hub_on_peak_da','houston_hub_off_peak_da')
	indiana_hub = data_scrub('Indiana Hub DA','indiana_hub_on_peak','indiana_hub_off_peak')

	# pulling hudson valley capacity prices and including trade_date for the final dataframe

	hud_val_cap = power_data.loc[power_data.HUB == 'NYISO Lower Hudson Valley', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]
	hud_val_cap = hud_val_cap.loc[hud_val_cap.PRODUCT == 'Capacity Futures', ['TRADE DATE', 'HUB','PRODUCT','STRIP','SETTLEMENT PRICE']]

	hud_val_cap['trade_date'] = hud_val_cap['TRADE DATE'] 
	hud_val_cap['strip'] = hud_val_cap['STRIP']
	hud_val_cap['hudson_valley_capacity'] = hud_val_cap['SETTLEMENT PRICE']

	hud_val_cap = hud_val_cap.loc[:,['trade_date','strip','hudson_valley_capacity']]

	# creating a list of the scrubbed dataframes so that they can be merged together 

	scrubbed_dfs = [ad_hub,zone_g,zone_f,mass_hub,houston_hub_rt,houston_hub_da,indiana_hub,hud_val_cap]

	# merging the list of dataframes
	# reduce function --> https://www.geeksforgeeks.org/reduce-in-python/

	merged_dfs = reduce(lambda left, right: pd.merge(left,right, how='outer',on='strip',sort=True),scrubbed_dfs)

	# filling in the trade_date column

	merged_dfs['trade_date'] = hud_val_cap.iloc[0,0]

	# create primary key

	merged_dfs['primary_key'] = merged_dfs['trade_date'].astype(str) + '-' + merged_dfs['strip'].astype(str)

	# reorder column names

	master_df = merged_dfs.loc[:,['trade_date','strip','ad_hub_on_peak','ad_hub_off_peak','zone_g_on_peak','zone_g_off_peak','zone_f_on_peak','zone_f_off_peak','mass_hub_on_peak','mass_hub_off_peak','houston_hub_on_peak_rt','houston_hub_off_peak_rt','houston_hub_on_peak_da','houston_hub_off_peak_da','indiana_hub_on_peak','indiana_hub_off_peak','hudson_valley_capacity','primary_key']]

	# exporting master_df to csv

	master_df.to_csv("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\outputs\\power_data.csv", index=False, encoding='utf-8')

	# create a connection and cursor

	conn = pg2.connect(database='automation_projects',user='postgres',password='XXXXX',host = '127.0.0.1')
	cur = conn.cursor()

	# drop any existence of the temporary table used to upset data into the database

	cur.execute('DROP TABLE IF EXISTS z_ice_power;')

	# create a temporary sql table that will be used to upsert the data into the primary table

	cur.execute("""CREATE TABLE z_ice_power(
		trade_date TIMESTAMP,
		strip TIMESTAMP,
		ad_hub_on_peak NUMERIC,
		ad_hub_off_peak NUMERIC,
		zone_g_on_peak NUMERIC,
		zone_g_off_peak NUMERIC,
		zone_f_on_peak NUMERIC,
		zone_f_off_peak NUMERIC,
		mass_hub_on_peak NUMERIC,
		mass_hub_off_peak NUMERIC,
		houston_hub_on_peak_rt NUMERIC,
		houston_hub_off_peak_rt NUMERIC,
		houston_hub_on_peak_da NUMERIC,
		houston_hub_off_peak_da NUMERIC,
		indiana_hub_on_peak NUMERIC,
		indiana_hub_off_peak NUMERIC,
		hudson_valley_capacity NUMERIC,
		primary_key VARCHAR,
		PRIMARY KEY (primary_key)
	);""")

	# upload the csv to the z_ice_power table in the database

	power_data_file = open('C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\outputs\\power_data.csv')

	import_data = """
	COPY z_ice_power FROM STDIN WITH
		CSV
		HEADER
		DELIMITER AS ','
	"""

	cur.copy_expert(sql=import_data,file=power_data_file)

	print('upserting data...')

	# upserting temp table into the primary table

	upsert = """
		INSERT INTO ice_power(trade_date,strip,ad_hub_on_peak,ad_hub_off_peak,zone_g_on_peak,zone_g_off_peak,zone_f_on_peak,zone_f_off_peak,mass_hub_on_peak,mass_hub_off_peak,houston_hub_on_peak_rt,houston_hub_off_peak_rt,houston_hub_on_peak_da,houston_hub_off_peak_da,indiana_hub_on_peak,indiana_hub_off_peak,hudson_valley_capacity,primary_key)
			SELECT  z_ice_power.trade_date,
					z_ice_power.strip,
					z_ice_power.ad_hub_on_peak,
					z_ice_power.ad_hub_off_peak,
					z_ice_power.zone_g_on_peak,
					z_ice_power.zone_g_off_peak,
					z_ice_power.zone_f_on_peak,
					z_ice_power.zone_f_off_peak,
					z_ice_power.mass_hub_on_peak,
					z_ice_power.mass_hub_off_peak,
					z_ice_power.houston_hub_on_peak_rt,
					z_ice_power.houston_hub_off_peak_rt,
					z_ice_power.houston_hub_on_peak_da,
					z_ice_power.houston_hub_off_peak_da,
					z_ice_power.indiana_hub_on_peak,
					z_ice_power.indiana_hub_off_peak,
					z_ice_power.hudson_valley_capacity,
					z_ice_power.primary_key
					
			FROM ( SELECT
					z_ice_power.trade_date,
					z_ice_power.strip,
					z_ice_power.ad_hub_on_peak,
					z_ice_power.ad_hub_off_peak,
					z_ice_power.zone_g_on_peak,
					z_ice_power.zone_g_off_peak,
					z_ice_power.zone_f_on_peak,
					z_ice_power.zone_f_off_peak,
					z_ice_power.mass_hub_on_peak,
					z_ice_power.mass_hub_off_peak,
					z_ice_power.houston_hub_on_peak_rt,
					z_ice_power.houston_hub_off_peak_rt,
					z_ice_power.houston_hub_on_peak_da,
					z_ice_power.houston_hub_off_peak_da,
					z_ice_power.indiana_hub_on_peak,
					z_ice_power.indiana_hub_off_peak,
					z_ice_power.hudson_valley_capacity,
					z_ice_power.primary_key
					FROM z_ice_power
			) z_ice_power

			ON CONFLICT (primary_key)

			DO UPDATE SET
					trade_date = EXCLUDED.trade_date,
					strip = EXCLUDED.strip,
					ad_hub_on_peak = EXCLUDED.ad_hub_on_peak,
					ad_hub_off_peak = EXCLUDED.ad_hub_off_peak,
					zone_g_on_peak = EXCLUDED.zone_g_on_peak,
					zone_g_off_peak = EXCLUDED.zone_g_off_peak,
					zone_f_on_peak = EXCLUDED.zone_f_on_peak,
					zone_f_off_peak = EXCLUDED.zone_f_off_peak,
					mass_hub_on_peak = EXCLUDED.mass_hub_on_peak,
					mass_hub_off_peak = EXCLUDED.mass_hub_off_peak,
					houston_hub_on_peak_rt = EXCLUDED.houston_hub_on_peak_rt,
					houston_hub_off_peak_rt = EXCLUDED.houston_hub_off_peak_rt,
					houston_hub_on_peak_da = EXCLUDED.houston_hub_on_peak_da,
					houston_hub_off_peak_da = EXCLUDED.houston_hub_off_peak_da,
					indiana_hub_on_peak = EXCLUDED.indiana_hub_on_peak,
					indiana_hub_off_peak = EXCLUDED.indiana_hub_off_peak,
					hudson_valley_capacity = EXCLUDED.hudson_valley_capacity;
	"""

	cur.execute(upsert)

	# committing the sql statements above

	conn.commit()

	print('power data scrubbed and stored in database...')