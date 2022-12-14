import requests
import json
import pandas as pd

print('processing...')

# create dataframe of excel

input_file = 'C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1 - working\\inputs\\RT Energy Imbalance.xlsx'

df_revenue = pd.read_excel(input_file, sheet_name = 'Imbalance chart')
df_generation = pd.read_excel(input_file, sheet_name = 'Settlement Volumes Chart')

url = 'https://ams.pharos-ei.com/api/ercot/lmp/historic/hourly?start_date=2022-12-01&end_date=2023-05-01&location_id=HB_HOUSTON,ROW_ALL'
key = '8190a10c7f7eeda592f0aKAgdwfBEr6oywcmHutrS'

df_power = requests.get(url,auth=(key,'*')).text
df_power = json.loads(df_power)
df_power = df_power['lmp']
df_power = pd.DataFrame(df_power)

# cleaning up revenue data

df_revenue = df_revenue.loc[:,['FlowdayAndInterval (Year > Month > Day > Hour > Minute)','Amount - Credit Positive']]

if df_revenue.iloc[0,0] == '(All)':
	df_revenue = df_revenue.drop(0)
else:
	pass

print(df_revenue)


df_revenue['date'] = pd.to_datetime(df_revenue['FlowdayAndInterval (Year > Month > Day > Hour > Minute)'], format = '%Y-%m-%d %X')
df_revenue['date'] = df_revenue['date'].dt.strftime('%Y-%m-%d')
df_revenue = df_revenue.groupby(['date'], as_index=False).sum()

print(df_revenue)


# creating master dataframes that are database ready

df_cutlass_margin = pd.DataFrame()

df_cutlass_margin['date'] = pd.Series(df_revenue['date'].values)
df_cutlass_margin['gen_revenue'] = pd.Series(df_revenue['Amount - Credit Positive'].values)
# df_cutlass_margin['gen_mw'] = 
# df_cutlass_margin['gen_mw_max'] = 
# df_cutlass_margin['rowland_lmp_min'] = 
# df_cutlass_margin['rowland_lmp_realized'] = 
# df_cutlass_margin['rowland_lmp_max'] = 
# df_cutlass_margin['rowland_basis'] = 

print(df_cutlass_margin)