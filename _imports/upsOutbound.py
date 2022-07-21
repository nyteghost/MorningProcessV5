import numpy as np
import pandas as pd
import getpass
import os
import shutil
import fnmatch
from openpyxl import load_workbook
import glob
import shutil
import xlsxwriter
import win32com.client
from datetime import datetime, timedelta, date, time
import pathlib
import time
from mpConfigs.logger_setup import setup_logger, log_location
from mpConfigs.dbConfig import dbConnect

today = date.today()
strtoday = str(today)
importlog = setup_logger(f'upsoutboundsimport',log_location + "\\" + "UPS Outbound" + "\\" + 'UPSOutboundImport {today}.log')

prefix = r"C:\Users"
localuser = getpass.getuser()
named_shipment = 'UPSShipmentsDaily.xlsx'
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database"
end_path = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets"
out_path = prefix + "\\" + localuser + end_path
processed_file = prefix + "\\" + localuser + r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Processed\Daily UPS Shipment"
excel_relative_file_path = out_path + '\\' + 'UPSShipmentsDaily.xlsx'

# Pandas Options
pd.options.display.max_columns = None
pd.options.display.width = None

## Rename Outbound File
for file in os.listdir(prefix + "\\" + localuser + "\\" + suffix):
    if fnmatch.fnmatch(file, 'outbound_*.csv'):
        print(file)
        read_file = pd.read_csv(prefix + "\\" + localuser + suffix + "\\" + file)
        read_file.to_excel(out_path + "\\" + named_shipment, index=None, header=True)
        shutil.move((prefix + "\\" + localuser + suffix + "\\" + file), processed_file)



print('SUCCESS: Connection to DB\nIN PROGRESS: Daily UPS Data Import')

###############  UPS DATA IMPORT #############
column_mapping = {
    'Tracking Number': 'Tracking Number',
    'Status': 'Status',
    'Package Reference No. 1': 'Package Reference No# 1',
    'Package Reference No. 2': 'Package Reference No# 2',
    'Manifest Date': 'Manifest Date',
    'Ship To Name': 'Ship To Name',
    'Ship To City': 'Ship To City',
    'Ship To State/Province': 'Ship To State/Province',
    'Ship To Postal Code': 'Ship To Postal Code',
    'Ship To Country or Territory': 'Ship To Country or Territory',
    'Service': 'Service',
    'Scheduled Delivery': 'Scheduled Delivery',
    'Exception Description': 'Exception Description',
    'Exception Status Description': 'Exception Status Description',
    'Exception Resolution': 'Exception Resolution',
    'Return To Sender Indicator': 'Return To Sender Indicator',
    'Date Delivered': 'Date Delivered',
    'Alternate Tracking Number': 'Alternate Tracking Number',
    'Shipment Activity Date': 'Shipment Activity Date'
}

# READING THE EXCEL FILE INTO A DATAFRAME
data = pd.read_excel(excel_relative_file_path)
excel_df = pd.DataFrame(data, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(data), None).replace('\.00', '', regex=True).replace('Not Avail.', '', regex=True)
importlog.info('Daily Shipment Import.')

connect = dbConnect("shippingliaison")
connect.df_to_sql(excel_df, 'dbo_upsrawdata')
connect.call('dbo_importrawdatasort')






