import os as os
import glob as glob
import win32com.client as wincom

def cutlass_email_scrub():

	print('processing...')

	# setting a variable for the path to the folder to save the attachments in

	target_folder = "Q:\\Option Model\\PythonScripts\\MarginReports\\Inputs\\"

	# Connecting to outlook

	outlook = wincom.Dispatch("Outlook.Application").GetNamespace("MAPI")

	# Connecting to the inbox

	inbox = outlook.Folders("bcasamassima@advancedpowerna.com").Folders("Cutlass Data")

	# setting the message variable to iterate through

	emails = inbox.Items

	email = emails.GetLast()
	#email = emails.GetFirst()

	attachments = email.Attachments

	attachment = attachments.Item(1)

	attachment.SaveAsFile(os.path.join(target_folder,str(attachment)))

	print('completed')

#cutlass_email_scrub()
