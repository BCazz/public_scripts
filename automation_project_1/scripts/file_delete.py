import os 
import glob

print('processing...')

os.chdir("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1 - working\\inputs")

cwd = os.getcwd()

files = glob.glob('*.xlsx')

for file in files:
	os.remove(file)

print('file(s) deleted')


