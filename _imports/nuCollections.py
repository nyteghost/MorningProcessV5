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
import time
from mpConfigs.dbConfig import dbConnect
import better_exceptions

better_exceptions.MAX_LENGTH = None
better_exceptions.hook()
today = date.today()
strtoday = str(today)
Date = today


logger.disable("my_library")
# logging.basicConfig(filename=f"..\\..\\logs\\collections {Date}.log", level=logging.ERROR)

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT - Collections Data.xlsx"
excel_relative_file_path = prefix + "\\" + localuser + suffix


# Student Data File Sheet1
column_mapping = {
    'PID': 'PID',
    'STID or StaffID': 'STID',
    'FID': 'FID',
    'FN': 'SFN',
    'LN': 'SLN',
    'DOB': 'DOB',
    'Collections Open': 'Collections Open',
    'Collections Closed': 'Collections Closed',
    'LG First': 'LG First',
    'LG Last': 'LG Last',
    'LG or Staff  Email': 'LG Email',
    'LG or Staff  Phone': 'LG Phone',
    'Unit Info': 'Unit Info',
    'Address': 'Address',
    'City': 'City',
    'State': 'State',
    'Zip Code': 'Zip Code',
    'Most Recent Withdrawl': 'Most Recent Withdrawl'
}
print('SUCCESS: Connection to DB\nIN PROGRESS: Daily Collections Import')

tic = time.time()

for collections_fileName_relative in glob.glob('Z:/*Collections*', recursive=True):
    data = pd.read_csv(collections_fileName_relative)
    data['FID'] = data['FID'].replace("staff", '')
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].replace("No LOE", '')
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].replace("Staff no WD", "")
    data['PID'] = np.floor(pd.to_numeric(data['PID'], errors='coerce')).astype('Int64')
    data['FID'] = np.floor(pd.to_numeric(data['FID'], errors='coerce')).astype('Int64')
    data['STID or StaffID'] = np.floor(pd.to_numeric(data['STID or StaffID'], errors='coerce')).astype('Int64')
    data['PID'] = data['PID'].astype('Int64')
    data['FID'] = data['FID'].astype('Int64')
    data['DOB'] = pd.to_datetime(data['DOB'])
    data['DOB'] = data['DOB'].dt.strftime('%Y-%m-%d')
    data['Collections Open'] = pd.to_datetime(data['Collections Open'])
    data['Collections Open'] = data['Collections Open'].dt.strftime('%Y-%m-%d')
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].replace(['Active'],'')
    data['Most Recent Withdrawl'] = pd.to_datetime(data['Most Recent Withdrawl'])
    data['Most Recent Withdrawl'] = data['Most Recent Withdrawl'].dt.strftime('%Y-%m-%d')

    # data['STID or StaffID'] = data['STID or StaffID'].astype('Int64')
    df = pd.DataFrame(data, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(data), None)
    df = df.rename(columns=column_mapping)
    print(df)
    # df.loc[(df['Most Recent Withdrawl'] == 'Active'), 'Most Recent Withdrawl'] = ''



    logging.info(
        'Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Inserting Data from Excel Sheet into the Collections Dataframe.')
    logging.info(df)

    df.columns = df.columns.str.lower()
    print(df)

    connect = dbConnect("gcaassetmgmt_2_0")

    connect.df_to_sql(df, 'stage_collectionsdata')

    connect.call('pers_uspupdatecollections')

    # fileName_absolute = os.path.basename(collections_fileName_relative)
    # new_name = "r" + str(Date) + 'r-' + fileName_absolute
    # shutil.move('Z:/' + fileName_absolute, 'Z:/Historical/' + new_name)
    # print(new_name + ' moved to Historical Folder.')
    # logging.info(new_name + ' moved to Historical Folder.')

    toc = time.time()
    print('Done in {:.4f} seconds'.format(toc - tic))

    logging.info(
        'Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Inserting Data from Excel Sheet into the Collections Dataframe.')
    logging.info(df)
