import os
import win32com.client as wincom

print('processing...')

# creating a variable for the folder where want the excel file moved to

target_folder = "C:\\Users\\bjcas\\Documents\\GitHub\\public_scripts\\automation_project_1 - working\\inputs"

print('connecting to outlook...')

# connecting to outlook

outlook = wincom.Dispatch("Outlook.Application").GetNamespace("MAPI")

# connecting to inbox

inbox = outlook.Folders("bcasamassima@casacode.com").Folders("auto_1")

print('pulling email attachement...')

# setting emails as an item/object to be called on

emails = inbox.Items

email = emails.GetLast()

attachments = email.Attachments

attachment = attachments.Item(1)

print('moving attachement to the inputs folder...')

attachment.SaveAsFile(os.path.join(target_folder,str("data.xlsx")))

# https://learn.microsoft.com/en-us/office/vba/api/overview/