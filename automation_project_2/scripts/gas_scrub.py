import pandas as pd 
import os as os 
import glob as glob
from functools import reduce
import psycopg2 as pg2

print('scrubbing gas data...')

# setting the working directory and looping through the power files

os.chdir("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs\\")

files = glob.glob('icecleared_gas*')

for file in files:

	# reading the excel file as a dataframe

	gas_data = pd.read_excel(file)

	# creating a dataframe for the desired henry hub data

	henry_hub = gas_data.loc[(gas_data.HUB == 'Henry') & (gas_data.PRODUCT == 'NG LD1 Futures'),['TRADE DATE','STRIP','SETTLEMENT PRICE','EXPIRATION DATE']]
	henry_hub = henry_hub.rename(columns={'TRADE DATE':'trade_date','STRIP':'strip','SETTLEMENT PRICE':'henry_hub','EXPIRATION DATE':'henry_hub_expiration'})

		# more on renaming:
			#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
			#henry_hub.columns = henry_hub.columns.str.lower().str.replace(" ", "_")
		
	def data_scrub(hub,name):

		# hub == the hub as named is the ice settlement file
		# name == the column name to be used in the database

		dataframe = gas_data.loc[(gas_data.HUB == hub) & (gas_data.PRODUCT == 'NG Basis LD1 for IF Futures'),['STRIP','SETTLEMENT PRICE']]
		dataframe = dataframe.rename(columns={'STRIP':'strip','SETTLEMENT PRICE':name})

		return dataframe

	# running thru various desired price points

	tgp_z4_200l = data_scrub('TGP-Z4 200L', 'tgp_z4_200l')
	eastern_gas_south = data_scrub('Eastern Gas-South','eastern_gas_south')
	iroquis_z2 = data_scrub('Iroquois-Z2','iroquis_z2')
	algonquin = data_scrub('Algonquin','algonquin')
	nwp_rockies = data_scrub('NWP-Rockies','nwp_rockies')

	# putting the dataframes into a list to be merged

	scrubbed_dfs = [henry_hub,tgp_z4_200l,eastern_gas_south,iroquis_z2,algonquin,nwp_rockies]

	# merging into one dataframe

	merged_dfs = reduce(lambda left,right: pd.merge(left,right,how='outer',on='strip',sort=True),scrubbed_dfs)

	# creating a primary key

	merged_dfs['primary_key'] = merged_dfs['trade_date'].astype(str) + '-' + merged_dfs['strip'].astype(str)

	# creating master/final dataframe

	master_df = merged_dfs.loc[:,['trade_date','strip','henry_hub','tgp_z4_200l','eastern_gas_south','iroquis_z2','algonquin','nwp_rockies','henry_hub_expiration','primary_key']]

	# exporting dataframe to csv

	master_df.to_csv("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\outputs\\gas_data.csv", index=False, encoding='utf-8')

	# create a connection and cursor

	conn = pg2.connect(database='automation_projects',user='postgres',password='XXXX',host = '127.0.0.1')
	cur = conn.cursor()

	# drop any existence of the temporary table used to upset data into the database

	cur.execute('DROP TABLE IF EXISTS z_ice_gas;')

	# create a temporary sql table that will be used to upsert the data into the primary table

	cur.execute("""CREATE TABLE z_ice_gas(
		trade_date TIMESTAMP,
		strip TIMESTAMP,
		henry_hub NUMERIC,
		tgp_z4_200l NUMERIC,
		eastern_gas_south NUMERIC,
		iroquis_z2 NUMERIC,
		algonquin NUMERIC,
		nwp_rockies NUMERIC,
		henry_hub_expiration TIMESTAMP,
		primary_key VARCHAR,
		PRIMARY KEY (primary_key)
	);""")

	# upload the csv to the z_ice_gas table in the database

	gas_data_file = open('C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\outputs\\gas_data.csv')

	import_data = """
	COPY z_ice_gas FROM STDIN WITH
		CSV
		HEADER
		DELIMITER AS ','
	"""

	cur.copy_expert(sql=import_data,file=gas_data_file)

	print('upserting data...')

	# upserting temp table into the primary table

	upsert = """
		INSERT INTO ice_gas(trade_date,strip,henry_hub,tgp_z4_200l,eastern_gas_south,iroquis_z2,algonquin,nwp_rockies,henry_hub_expiration,primary_key)
			SELECT  z_ice_gas.trade_date,
					z_ice_gas.strip,
					z_ice_gas.henry_hub,
					z_ice_gas.tgp_z4_200l,
					z_ice_gas.eastern_gas_south,
					z_ice_gas.iroquis_Z2,
					z_ice_gas.algonquin,
					z_ice_gas.nwp_rockies,
					z_ice_gas.henry_hub_expiration,
					z_ice_gas.primary_key
					
			FROM ( SELECT
					z_ice_gas.trade_date,
					z_ice_gas.strip,
					z_ice_gas.henry_hub,
					z_ice_gas.tgp_z4_200l,
					z_ice_gas.eastern_gas_south,
					z_ice_gas.iroquis_Z2,
					z_ice_gas.algonquin,
					z_ice_gas.nwp_rockies,
					z_ice_gas.henry_hub_expiration,
					z_ice_gas.primary_key
					FROM z_ice_gas
			) z_ice_gas

			ON CONFLICT (primary_key)

			DO UPDATE SET
					trade_date = EXCLUDED.trade_date,
					strip = EXCLUDED.strip,
			        henry_hub = EXCLUDED.henry_hub,
			        tgp_z4_200l = EXCLUDED.tgp_z4_200l,
			        eastern_gas_south = EXCLUDED.eastern_gas_south,
			        iroquis_z2 = EXCLUDED.iroquis_z2,
			        algonquin = EXCLUDED.algonquin,
			        nwp_rockies = EXCLUDED.nwp_rockies,
			        henry_hub_expiration = EXCLUDED.henry_hub_expiration;
	"""

	cur.execute(upsert)

	# committing the sql statements above

	conn.commit()

print('gas data scrubbed and stored in database...')