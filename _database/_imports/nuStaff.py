import pandas as pd
import getpass
import glob
from datetime import date
from loguru import logger
import better_exceptions
import logging
import time
from mpConfigs.dbConfig import dbConnect

better_exceptions.MAX_LENGTH = None
better_exceptions.hook()

today = date.today()
strtoday = str(today)
Date = today

logger.disable("my_library")
logging.basicConfig(filename=f"..\\..\\logs\\staff {Date}.log", level=logging.ERROR)

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT GCA Staff Data(From CSV).xlsx"
staff_excel_output = prefix + "\\" + localuser + suffix

#####################################################################################################################################################
# banana()
#####################################################################################################################################################

# The DB connection string is --

# Staff Updates Sheet
column_mapping = {
    'STF PID': '"STF PID"',
    'STF ID': '"STF ID"',
    'STF First': '"STF First"',
    'STF Last': '"STF Last"',
    'STF Hire': '"STF Hire"',
    'STF Term': '"STF Term"',
    'Current Title': '"Current Title"',
    'STF Pers Phone': '"STF Pers Phone"',
    'STF Pers Email': '"STF Pers Email"',
    'STF Work Phone': '"STF Work Phone"',
    'STF Ext': '"STF Ext"',
    'GA Cyber Email': '"GA Cyber Email"',
    'Online Email': '"Online Email"',
    'STF Street': '"STF Street"',
    'STF City': '"STF City"',
    'STF St': '"STF St"',
    'STF Zip': '"STF Zip"',
    'Staff Type': '"Staff Type"',
    'Office Staff': '"Office Staff"',
    'Shipment Hold': '"Shipment Hold"',
    'Student Chromebook Start': '"Student Chromebook Start"',
    'Student Chromebook End': '"Student Chromebook End"',
    'Student Windows Laptop Start': '"Student Windows Laptop Start"',
    'Student Windows Laptop End': '"Student Windows Laptop End"',
    'Student CTAE Laptop Start': '"Student CTAE Laptop Start"',
    'Student CTAE Laptop End': '"Student CTAE Laptop End"',
    'Desktop Start': '"Desktop Start"',
    'Desktop End': '"Desktop End"',
    'Hotspot Authorization': '"Hotspot Authorization"',
    'Monitor Requested': '"Monitor Requested"',
    'External Monitor Authorization': '"External Monitor Authorization"',
    'Duplicate Printer Authorization': '"Duplicate Printer Authorization"',
    'Declined Printer': '"Declined Printer"',
    'Latitude 5500': '"Latitude 5500"',
    'Y510': 'Y510',
    'Zoomy': 'Zoomy',
    'Collections Start': '"Collections Start"',
    'Collections End': '"Collections End"',
}

print('SUCCESS: Connection to DB\nIN PROGRESS: Daily Staff Import')
tic = time.time()

process_query = f"EXEC GCAAssetMGMT_2_0.[Pers].[uspUpdateStaffData];"

try:
    for staff_fileName_relative in glob.glob('Z:/*Staff*', recursive=True):
        if staff_fileName_relative:
            print('Attempting Data import')
            data = pd.read_csv(staff_fileName_relative)
            data = data.replace("Null", '')
            df = pd.DataFrame(data, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(data), None).replace('\.0', '', regex=True)
            print(df)

            connect = dbConnect("gcaassetmgmt_2_0")
            connect.df_to_sql(df, 'stage_staffdata')
            connect.call('pers.uspupdatestaffdata')
            df.to_excel(staff_excel_output)

            # fileName_absolute = os.path.basename(staff_fileName_relative)
            # new_name = "r" + str(Date) + 'r-' + fileName_absolute
            # shutil.move('Z:/' + fileName_absolute, 'Z:/Historical/' + new_name)
            # print(new_name + ' moved to Historical Folder.')
            # logging.info(new_name + ' moved to Historical Folder.')
except Exception as e:
    raise Exception("Unable to run staff sheet import" + str(e))

toc = time.time()
print('Done in {:.4f} seconds'.format(toc - tic))
