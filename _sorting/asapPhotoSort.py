import getpass
from alive_progress import alive_bar
import os
from openpyxl import load_workbook
import win32com.client
from datetime import datetime, timedelta, date, time
import pathlib
import time
import os, sys
import logging
from loguru import logger

today = date.today()
strtoday = str(today)
Date = today

logger.disable("my_library")
# logging.basicConfig(filename=f"./logs/asap_photo_sort {Date}.log", level=logging.ERROR)

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Pickup Data"
attachment_file_path = prefix + "\\" + localuser + suffix
outlook = win32com.client.Dispatch('outlook.application')
mapi = outlook.GetNamespace('MAPI')

# Settings for folders
inbox = mapi.Folders['Warehouse'].Folders['Inbox']
imagebox = mapi.Folders['Warehouse'].Folders['Images']
onboardbox = mapi.Folders['Warehouse'].Folders['Auto Onboard Alerts']
podbox = mapi.Folders['Warehouse'].Folders['Auto POD Alerts']
messages = inbox.Items

subject = "POD Image File"  # Subject line to Filter
onboard_subject = "Auto Onboard Alert"
pod_subject = "Auto POD Alert"

# Set to only pull last 24 hours
received_dt = datetime.now() - timedelta(days=7)
received_dt = received_dt.strftime('%m/%d/%Y')

# Restrictions
messages = messages.Restrict("[ReceivedTime] >= '" + received_dt + "'")
messages = messages.Restrict("[SenderEmailAddress] = 'dispatch@asapcourier.us'")
received_dt = received_dt.replace("/", "-")

asap_list = []

# Loops through emails looking for Specific Subject, and pulls the FID from the Email
try:
    for message in list(messages):
        if subject in message.Subject:
            if message.Unread:
                message.UnRead = False  # Marks the emails as read
            print(message.Subject, message.ReceivedTime)
            msgdate = str(message.ReceivedTime)
            msgdate = msgdate[:10]
            os.makedirs(attachment_file_path + '\\' + time.strftime(msgdate), exist_ok=True)  # Creates directory based on Date Received
            new_dir_name = pathlib.Path(attachment_file_path + '\\' + time.strftime(msgdate), exist_ok=True)
            new_dir_name = str(new_dir_name)
            body_content = message.body
            keyword = 'Reference'  # Uses reference as the keyword to discover the keyword after Reference which should be the Family ID
            before_keyword, keyword, after_keyword = message.body.partition(keyword)  # Partitions the lines in the email to make it easier to parse
            keyword = after_keyword  # Makes everything After Reference the keyword
            keyword = keyword.splitlines()[0]  # Uses index to only keep the first Keyword line which has The Family ID in it.
            keyword = keyword.replace(':', '')  # Replaces any : with blank space
            keyword = keyword.replace(' ', '')  # Replaces any space with a blank space
            print(keyword)
            asap_list.append(keyword)
            # Any emails that meet the requirements above, have the attachment downloaded to a folder that is created by date and then FID
            try:
                s = message.sender
                for attachment in message.Attachments:
                    os.makedirs(new_dir_name + '\\' + keyword, exist_ok=True)  # Creates a directory based on the Keyboard
                    new_fid_name = pathlib.Path(new_dir_name + '\\' + keyword, exist_ok=True)
                    new_fid_name = str(new_fid_name)
                    attachment.SaveASFile(os.path.join(
                        new_fid_name + '\\' + "FID " + keyword + " " + attachment.FileName))  # saves attachment
                    print(f"attachment {attachment.FileName} from {s} saved")
                    message.Move(imagebox)  # Moves the email to the Images Folder in Outlook
                    break
            except Exception as e:
                logging.error("error when saving the attachment:" + str(e))
        elif pod_subject in message.Subject:
            message.UnRead = False  # Marks the emails as read
            message.Move(podbox)  # Moves the email to the Auto Pod Alerts Folder in Outlook
        elif onboard_subject in message.Subject:
            message.UnRead = False  # Marks the emails as read
            message.Move(onboardbox)  # Moves the email to the Auto Onboard Alert Folder in Outlook

        else:
            pass
    print(len(asap_list))
    logging.info(str(len(asap_list)) + " ASAP returns downloaded and moved to the correct folder by Family ID.")
    for i in asap_list:
        logging.info(i)
except Exception as e:
    logging.error("error when saving the attachment:" + str(e))

