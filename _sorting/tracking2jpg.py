import win32com.client
import getpass
import os
import time
import pathlib
import imgkit
import shutil
from loguru import logger
from datetime import datetime, timedelta, date

logger.add("logger.log", backtrace=True, diagnose=True, rotation="12:00")

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\UPS POD Data"
attachment_file_path = prefix + "\\" + localuser + suffix
config = imgkit.config(wkhtmltoimage=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe')

outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
folder = outlook.Folders.Item("shipping@scacloud.com")
inbox = folder.Folders.Item("Inbox")
deliveryFolder = inbox.Folders["Delivery"]
messages = deliveryFolder.Items

subject = "Delivery"

# Set to only pull last 24 hours
received_dt = datetime.now() - timedelta(days=1)
received_dt = received_dt.strftime('%m/%d/%Y')

# Restrictions
messages = messages.Restrict("[ReceivedTime] >= '" + received_dt + "'")

ups_list = []

print(len(messages))

for message in list(messages):
    if subject in message.Subject:
        msgDate = str(message.ReceivedTime)
        msgDate = msgDate[:10]
        split_message = str(message).split()
        os.makedirs(attachment_file_path + '\\' + time.strftime(msgDate), exist_ok=True)
        new_dir_name = pathlib.Path(attachment_file_path + '\\' + time.strftime(msgDate), exist_ok=True)
        new_dir_name = str(new_dir_name)
        for tn in split_message:
            if "1Z" in tn:
                save_name = new_dir_name + '\\' + tn + '.html'
                jpg_name = new_dir_name + '\\' + tn + '.jpg'
                html_files = new_dir_name + '\\' + tn + '_files'
                isHTML = os.path.isfile(save_name)
                isJPG = os.path.isfile(jpg_name)

                if isJPG:
                    print('Already exists')
                    pass
                else:
                    if isHTML:
                        print('Found File')
                    else:
                        try:
                            message.SaveAS(save_name, 5)  # OlSaveAsType 5 html
                            print(save_name)
                        except Exception as err:
                            logger.error(err)
                            pass
                    if not isJPG:
                        ok = imgkit.from_file(save_name, jpg_name, config=config)
                        if ok:
                            print("successful")
                            print(jpg_name)
                            try:
                                os.remove(save_name)
                            except PermissionError as err:
                                time.sleep(5)
                                try:
                                    os.remove(save_name)
                                except Exception as err:
                                    logger.error(err)
                                    pass
                            try:
                                shutil.rmtree(html_files)
                            except OSError as err:
                                logger.error(err)

                        else:
                            print("failed")
                    else:
                        os.remove(save_name)
                        try:
                            shutil.rmtree(html_files)
                        except OSError as err:
                            logger.error(err)
                    # message.Delete()
