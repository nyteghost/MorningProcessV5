import numpy as np
import pandas as pd
import getpass
from alive_progress import alive_bar
import glob
import shutil
from datetime import date
import os
from util_lib import cherry
import logging
from loguru import logger
from dbConfig import con_to_db, df_to_sql
today = date.today()
strtoday = str(today)
Date = today

logger.disable("my_library")
logging.basicConfig(filename=f"./logs/collections {Date}.log", level=logging.ERROR)

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT - Collections Data.xlsx"
excel_relative_file_path = prefix + "\\" + localuser + suffix

strtoday = str(today)

# Pandas Options
# pd.options.display.max_columns = None
# pd.options.display.width=None
#####################################################################################################################################################
cherry()
#####################################################################################################################################################

# The DB connection string is --

cursor = conn.cursor()


###############  Student Data File Sheet1  #############
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
attempts = 0
while attempts < 1:
    try:
        data = pd.read_excel(excel_relative_file_path)
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
        # print(excel_df)
        # print('EXCEL DF', '\n', excel_df, excel_df.dtypes)

        # COMMON VARIABLES
        table_name = 'GCAAssetMGMT_2_0.Stage.CollectionsData'  # name of primary table to work with in database
        select_fields = ", ".join(column_mapping.values())
        values_list = ", ".join('?' * len(column_mapping.values()))
        insert_query = f"INSERT INTO {table_name} ({select_fields}) VALUES ({values_list})"
        delete_query = f"DELETE FROM {table_name};"
        process_query = f"EXEC GCAAssetMGMT_2_0.Pers.uspUpdateCollections;"

        # empty specified table prior to import
        try:
            cursor.execute(delete_query)
        except:
            print("Unable to delete query")
            logging.info("Unable to delete query")
            raise Exception("Unable to delete query")

        # Insert each row from Master Updater
        #  into the specified table
        print('Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Weekly Collections Import.')
        print(excel_df)

        logging.info(
            'Success: Deleting TEMPCollectionsData Table Content\nIN PROGRESS: Inserting Data from Excel Sheet into the Collections Dataframe.')
        logging.info(excel_df)
        with alive_bar(len(excel_df.index)) as bar:
            try:
                for index, row in excel_df.iterrows():
                    cursor.execute(insert_query, *row)
                    bar()
            except pyodbc.ProgrammingError as error:
                print(error)
                raise Exception(error)

        print("Updating SQL")

        # logger.info('SQL Would be updating, but cursor.execute(process_query) is still commented out!')
        cursor.execute(process_query)

        conn.commit()

        # Show the Collections Proc to confirm executed correctly
        collections_query = f"select * FROM GCAAssetMGMT_2_0.Stage.CollectionsData"
        collections_show = pd.read_sql(collections_query, conn)
        logging.info("Below are the results of the TEMPCollectionsData Proc")
        logging.info(collections_show)
        print(collections_show)
        conn.close()

        print('Good to Go')
        logging.info("New data has been loaded into the '" + table_name + "' table.")
        for fileName_relative in glob.glob('Z:/*Collections*', recursive=True):
            fileName_absolute = os.path.basename(fileName_relative)
            os.rename(fileName_relative, 'Z:/' + str(Date) + '-' + fileName_absolute)
            new_name = str(Date) + '-' + fileName_absolute
            shutil.move('Z:/' + new_name, 'Z:/Historical/' + new_name)
            print(new_name + ' moved to Historical Folder.')
            logging.info(fileName_absolute + 'moved to Historical Folder.')
    except ValueError as e1:
        print("Error: there was a Value error")
        logging.error(e1)
        attempts += 1
    except Exception as e:
        print(e)
        logging.error("//// THERE WAS AN ERROR. SEE BELOW ////")
        logging.error("\n\n\n")
        logging.error(e)
        logging.error(e)
        logging.error("\n\n\n")
        logging.error("//// THERE WAS AN ERROR. SEE ABOVE ////")
        attempts += 1
        raise Exception(e)
    else:
        break
