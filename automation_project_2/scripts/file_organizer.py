import os
import glob
import shutil


print('moving scrubbed files to already_scrubbed folder...')

# set the current working directory

os.chdir("C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs")

# creating a variable of the files to be looped thru

files = glob.glob('icecleared_*')

# moving each file to the desired folder

for file in files:

	shutil.move(file,"C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_2\\inputs\\already_scrubbed")

print('files moved to the already_scrubbed folder...')