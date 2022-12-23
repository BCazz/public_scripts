import pandas as pd
import psycopg2 as pg2
import datetime as dt
import xlwings as xlw
import os 
import shutil

# https://docs.python.org/3/library/shutil.html
# https://docs.xlwings.org/en/stable/api.html

print('connecting to the database...')

# create a connection and cursor

conn = pg2.connect(database='automation_projects',user='postgres',password='Bcazz1',host = '127.0.0.1')
cur = conn.cursor()

print('pulling out gas and power data...')

# create sql queries

ice_gas_query = """
	SELECT * FROM ice_gas;
"""

ice_power_query = """
	SELECT * FROM ice_power;
"""

# use these queries to create a dataframe

ice_gas_data = pd.read_sql(ice_gas_query,conn)
ice_power_data = pd.read_sql(ice_power_query,conn)

# closing connection to the database

cur = conn.close()

print('creating varaibles for time filters...')

# create variables equal to the date of the data to be used

	# time delta --> https://docs.python.org/3/library/datetime.html

#trade_date = dt.date.today()
#trade_date = dt.date.today() - dt.timedelta(days=1)
trade_date = dt.date(2022,12,16)

# create a datetime object to be used a start date filter for strip

start_date = trade_date + pd.offsets.MonthBegin(n=1)

# convert datetime variables to pandas datetime objects

trade_date = pd.to_datetime(trade_date, format= '%Y-%m-%d')
start_date = pd.to_datetime(start_date, format= '%Y-%m-%d')

# creating a variable equal to the henry_hub contract expiration date to use to skip over a row when pasting in the henry hub price at the end of the month

expiration_dates = ice_gas_data['henry_hub_expiration']
expiration_dates = pd.to_datetime(ice_gas_data['henry_hub_expiration'], format= '%Y-%m-%d')
expiration_dates = expiration_dates.drop_duplicates()
expiration_dates = expiration_dates.dropna()

for date in expiration_dates:
	if date.month == trade_date.month and date.year == trade_date.year:
		henry_hub_expiration_date = date
	else:
		pass

# reformatting datetime series to match trade date and start date format

ice_gas_data['trade_date'] = pd.to_datetime(ice_gas_data['trade_date'], format= '%Y-%m-%d')
ice_gas_data['strip'] = pd.to_datetime(ice_gas_data['strip'], format= '%Y-%m-%d')
ice_power_data['trade_date'] = pd.to_datetime(ice_gas_data['trade_date'], format= '%Y-%m-%d')
ice_power_data['strip'] = pd.to_datetime(ice_gas_data['strip'], format= '%Y-%m-%d')

# sorting dataframes

	# sort values --> https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html

ice_gas_data = ice_gas_data.sort_values(by = ['trade_date','strip'], ascending = [True,True])
ice_power_data = ice_power_data.sort_values(by = ['trade_date','strip'], ascending = [True,True])

print('connecting to the excel model...')

# connecting xlwings to os

app = xlw.App(visible=True)

# connecting to excel file 

# xlwings book objects --> https://docs.xlwings.org/en/stable/api.html
# xlwings sheets -->

model = xlw.Book("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs\\model.xlsm", update_links = False)

# creating sheets objects

xlw.sheets

# setting specific variable for input sheet

inputs_sheet = model.sheets['Inputs']
inputs_sheet.range("D2").value = trade_date

# setting a variable for the ice curves sheet

ice_curves_sheet = model.sheets['ICE Curves']

# creating function to filter out the specific/desired price points

def date_filters(dataframe,price_point):

	series = dataframe.loc[(dataframe['trade_date'] == trade_date) & (dataframe['strip'] >= start_date),price_point]

	return series

# passing the price points thru the function

ad_hub_on_peak = date_filters(ice_power_data,'ad_hub_on_peak')
ad_hub_off_peak = date_filters(ice_power_data,'ad_hub_off_peak')
henry_hub = date_filters(ice_gas_data,'henry_hub')
eastern_gas_south = date_filters(ice_gas_data,'eastern_gas_south')
tgp_z4_200l = date_filters(ice_gas_data,'tgp_z4_200l')

print('inputting data into the model...')

# pasting in the power and gas data
	
	# paste options --> https://docs.xlwings.org/en/stable/api.html#xlwings.Range.options

ice_curves_sheet.range("H4").options(pd.Series, header = False, index = False).value = ad_hub_on_peak
ice_curves_sheet.range("P4").options(pd.Series, header = False, index = False).value = ad_hub_off_peak

if trade_date > henry_hub_expiration_date:
	henry_hub = henry_hub.iloc[1:]
	ice_curves_sheet.range("X5").options(pd.Series, header = False, index = False).value = henry_hub
else:
	ice_curves_sheet.range("X4").options(pd.Series, header = False, index = False).value = henry_hub

ice_curves_sheet.range("AF4").options(pd.Series, header = False, index = False).value = eastern_gas_south
ice_curves_sheet.range("AN4").options(pd.Series, header = False, index = False).value = tgp_z4_200l

print('recalculating and saving the model...')

# calculate the excel spreadsheet

app.calculate()

# saving the model and close it

model.save()
model.close()

# reformatting trade_date for excel naming

trade_date = trade_date.strftime('%m_%d_%Y')

# saving down a copy of the updated model and renaming it

current_path = 'C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs\\'
history_path = 'C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\outputs\\model_history'

model_file = 'model.xlsm'
model_history_file = trade_date+'_model.xlsm'

shutil.copy(os.path.join(current_path,model_file),os.path.join(history_path,model_file))
os.rename(os.path.join(history_path,model_file),os.path.join(history_path,model_history_file))


