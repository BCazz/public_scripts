import os as os
import glob as glob
import win32com.client as wincom

def file_delete():

	print('processing...')

	os.chdir("Q:\\Option Model\\PythonScripts\\MarginReports\\Inputs")

	cwd = os.getcwd()

	cutlass_files = glob.glob('RT Energy*')

	for file in cutlass_files:
		os.remove(file)

	print('completed')

#file_delete()
