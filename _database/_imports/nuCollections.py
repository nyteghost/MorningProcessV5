import numpy as np
import pandas as pd
import getpass
from alive_progress import alive_bar
import glob
import shutil
from datetime import date
import os
import logging
from loguru import logger
from dbConfig import con_to_db, df_to_sql

today = date.today()
strtoday = str(today)
Date = today


logger.disable("my_library")
logging.basicConfig(filename=f"..\\..\\logs\\collections {Date}.log", level=logging.ERROR)

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT - Collections Data.xlsx"
excel_relative_file_path = prefix + "\\" + localuser + suffix

# Pandas Options
# pd.options.display.max_columns = None
# pd.options.display.width=None
#####################################################################################################################################################
# cherry()
#####################################################################################################################################################

# The DB connection string is --
conn = con_to_db("isolatedsafety")


# Student Data File Sheet1
column_mapping = {
    'PID': 'PID',
    'STID or StaffID': 'STID',
    'FID': 'FID',
    'FN': 'SFN',
    'LN': 'SLN',
    'DOB': 'DOB',
    'Collections Open': '"Collections Open"',
    'Collections Closed': '"Collections Closed"',
    'LG First': '"LG First"',
    'LG Last': '"LG Last"',
    'LG or Staff  Email': '"LG Email"',
    'LG or Staff  Phone': '"LG Phone"',
    'Unit Info': '"Unit Info"',
    'Address': 'Address',
    'City': 'City',
    'State': 'State',
    'Zip Code': '"Zip Code"',
    'Most Recent Withdrawl': '"Most Recent Withdrawl"'
}
print('SUCCESS: Connection to DB\nIN PROGRESS: Daily Collections Import')

for collections_fileName_relative in glob.glob('Z:/*Collections*',recursive=True):
    data = pd.read_csv(collections_fileName_relative)
    data['FID'] = data['FID'].replace("staff", '')
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].replace("No LOE", '')
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].replace("Staff no WD", "")
    data['PID'] = np.floor(pd.to_numeric(data['PID'], errors='coerce')).astype('Int64')
    data['FID'] = np.floor(pd.to_numeric(data['FID'], errors='coerce')).astype('Int64')
    data['STID or StaffID'] = np.floor(pd.to_numeric(data['STID or StaffID'], errors='coerce')).astype('Int64')
    data['PID'] = data['PID'].astype('Int64')
    data['FID'] = data['FID'].astype('Int64')
    data['STID or StaffID'] = data['STID or StaffID'].astype('Int64')
    excel_df = pd.DataFrame(data, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(data), None)
    excel_df.loc[(excel_df['Most Recent Withdrawl'] == 'Active'), 'Most Recent Withdrawl'] = ''

    print(excel_df)
    logging.info(
        'Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Inserting Data from Excel Sheet into the Collections Dataframe.')
    logging.info(excel_df)

    # Inserting Data from Excel Sheet into the Collections Dataframe
    df_to_sql(conn, excel_df, "stage_collectionsdata")
    conn.execute('CALL gcaassetmgmt_2_0.pers_uspupdatecollections')

    logging.info(
        'Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Inserting Data from Excel Sheet into the Collections Dataframe.')
    logging.info(excel_df)
