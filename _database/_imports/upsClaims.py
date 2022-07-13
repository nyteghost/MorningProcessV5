import numpy as np
import pandas as pd
import pyodbc
import getpass
from alive_progress import alive_bar
import os, sys
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
import os
from mpConfigs.logger_setup import setup_logger, log_location
from mpConfigs.dbConfig import dbConnect

today = date.today()
strtoday = str(today)

importlog = setup_logger(f'upsclaimssimport', log_location + "\\" + "UPS Claims" + '\\' + 'UPSClaimsImport {today}.log')

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database\claim_submission_summary.xlsx"
suffix2 = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database\Report.xlsx"
processed_folder = prefix + "\\" + localuser + r"\\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Processed\Claims"
excel_relative_file_path = prefix + "\\" + localuser + suffix
excel_relative_file_path2 = prefix + "\\" + localuser + suffix2

# Column mapping used to name the columns
column_mapping = {
    'Claim Number': '"Claim Number"',
    'Claim File Date': '"Claim File Date"',
    'Reason for Claim': '"Reason for Claim"',
    'Tracking Number/BOL': '"Tracking Number/BOL"',
    'Ship Date': '"Ship Date"',
    'Claim Status': '"Claim Status"',
    'Total Paid Amount': '"Total Paid Amount"',
    'Reference #': '"Reference #"',
    'Merchandise Description': '"Merchandise Description"',
    'Sender Name': '"Sender Name"',
    'Sender City': '"Sender City"',
    'Sender State': '"Sender State"',
    'Receiver Name': '"Receiver Name"',
    'Receiver City': '"Receiver City"',
    'Receiver State': '"Receiver State"',
    'Contact': 'Contact',
    'Contact Email': '"Contact Email"',
    'Contact Phone': '"Contact Phone"',
    'Adjuster': 'Adjuster',
    'Adjuster Email': '"Adjuster Email"',
    'Adjuster Phone': '"Adjuster Phone"',
}
########################################################################

# Read the files into pandas dataframes
claims = pd.read_excel(excel_relative_file_path, header=1)
claims = claims.iloc[:-1, :]
report = pd.read_excel(excel_relative_file_path2)
########################################################################

# Reads the Dataframe and sets the column names
claims_df = pd.DataFrame(claims, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(claims), None).replace('Not Avail.', '', regex=True)
claims_df['Reference #'] = claims_df['Reference #'].str.replace('\.0$', '')
########################################################################

# Creates the report dataframe and renames the Columns
report_df = pd.DataFrame(report)
report_df.rename(columns={
    'Claim #': 'Claim Number',
    'Inquiry Date': 'Claim File Date',
    'Claim Type': 'Reason for Claim',
    'Tracking #': 'Tracking Number/BOL',
    'Received By Name': 'Ship Date',
    'Claim Status': 'Claim Status',
    'Paid Amount': 'Total Paid Amount',
    'Shipper Reference #': 'Reference #',
    'Status Detail': 'Merchandise Description',
    'UPS Account Number': 'Sender Name',
    'Check Number': 'Sender City',
    'Paid Date': 'Sender State',
}, inplace=True)
########################################################################
# Stacks the Dataframes
all_df_list = [claims_df, report_df]
appended_df = pd.concat(all_df_list, ignore_index=True)
appended_df.fillna('', inplace=True)
########################################################################
# Write the appended dataframe to an excel file
out_path = prefix + "\\" + localuser + r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT - claim_submission_summary.xlsx"
appended_df.to_excel(out_path, index=False)
########################################################################

# Moves Files to New Folder
shutil.move(excel_relative_file_path, processed_folder)
shutil.move(excel_relative_file_path2, processed_folder)

# Renames Files with Time Stamp
old_claim = processed_folder + '\\' + 'claim_submission_summary.xlsx'
old_report = processed_folder + '\\' + 'Report.xlsx'

t = os.path.getctime(old_claim)
t_str = time.ctime(t)
t_obj = time.strptime(t_str)
form_t = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)
form_t = form_t.replace(":", "꞉")
os.rename(old_claim, os.path.split(old_claim)[0] + '/' 'claim_submission_summary - ' + form_t + os.path.splitext(old_claim)[1])
os.rename(old_report, os.path.split(old_report)[0] + '/' + 'Report - ' + form_t + os.path.splitext(old_report)[1])

importlog.info("Reading the UPS Claims Excel sheet into the Dataframe.")
print(appended_df)
importlog.info(appended_df)

connect = dbConnect("shippingliaison")
connect.df_to_sql(appended_df, 'dbo_dailyclaimssummary')
connect.call('uspupdateclaimssummarydata')
