import csv
import pandas as pd
import getpass
from itertools import zip_longest
from datetime import datetime, date
import glob
import shutil
from contextlib import redirect_stderr
import io
import logging
from loguru import logger
import time
import os
from mpConfigs.dbConfig import dbConnect

today = date.today()
strtoday = str(today)
Date = today

noHyphenDate = str(Date).replace('-', '')
print(noHyphenDate)


logger.disable("my_library")
logging.basicConfig(filename=f"..\\..\\logs\\nuStudent {Date}.log", level=logging.ERROR)


def UniqueResults(dataframe):
    tmp = [dataframe[col].unique() for col in dataframe]
    return pd.DataFrame(zip_longest(*tmp), columns=dataframe.columns)


today = datetime.today().strftime('%Y-%m-%d')

tic = time.time()

# Locations
prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Student Data(fromCSV).xlsx"
excel_relative_file_path = prefix + "\\" + localuser + suffix

prefix = r"C:\Users"
localuser = getpass.getuser()
student_sheet = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT GCA Student Data.xlsx"
to_student_sheetcsv = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT GCA Student Data.csv"
student_sheet_fromCSV = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\CURRENT GCA Student Data(from CSV).xlsx"
output = prefix + "\\" + localuser + fr'\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\Error Reports\{today} - Student Data Duplicates.xlsx'
extraColumnError = prefix + "\\" + localuser + fr'\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\Error Reports\{today} - Student Data Extra Column.csv'
goodPrefixStuff = fr"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\MySQLTests\goodStuff.csv"
student_excel_relative_file_path = prefix + "\\" + localuser + student_sheet_fromCSV
student_sheetcsv = prefix + "\\" + localuser + to_student_sheetcsv
goodStuff = prefix + "\\" + localuser + goodPrefixStuff

t = datetime.now()
log_date = t.strftime('%Y-%m-%d')

error_liens = []
good_lines = []
print(f"Z:/*SCA_21-22_Final{noHyphenDate}")
for student_fileName_relative in glob.glob(f'Z:/*SCA_21-22_Final{noHyphenDate}*', recursive=True):
    if student_fileName_relative is None:
        print('There was not a New Student Data sheet in Z Drive.')
        raise Exception("No nuStudentSheetFound")
    else:
        print('There is a student sheet.')
        f = io.StringIO()
        with redirect_stderr(f):
            with open(student_fileName_relative, 'r') as file:
                reader = csv.reader(file, quotechar='"')
                # if the first line is header
                header = 0
                for row in reader:
                    if header == 0:
                        header = 1
                        print(len(row))
                        error_liens.append(",".join(row))
                        good_lines.append(",".join(row))
                        continue
                    # checking if number of columns is not equal to 3 (bad lines)
                    if len(row) > 72:
                        error_liens.append(",".join(row))
                        good_lines.append(",".join(row[:9]) + strtoday)

                    else:
                        good_lines.append(",".join(row))

            # write the error list to error file
            errorWrite = open(extraColumnError, "w")
            errorWrite.write("\n".join(error_liens))
            errorWrite.close()

            goodWrite = open(goodStuff, "w")
            goodWrite.write("\n".join(good_lines))
            goodWrite.close()

        df = pd.read_csv(
            goodStuff, on_bad_lines='skip', index_col=False, low_memory=False)
        if f.getvalue():
            logger.warning("Had parsing errors: {}".format(f.getvalue()))
        # df = pd.read_csv(student_fileName_relative,on_bad_lines='warn',index_col=False)

        # df = df.iloc[:, :df.columns.get_loc('HOLD SHIPMENTS FOR ADDRESS') + 1]

        # print(df)
        # for col in df.columns:
        #     print(col)

        df.to_excel(student_excel_relative_file_path, index=None, header=True)
        shutil.copy(student_fileName_relative, student_sheetcsv)
        print(student_sheetcsv)

        # SQL VARIABLES
        process_query = f"EXEC GCAAssetMGMT_2_0.[Pers].[uspUpdateStudentData]"

        column_mapping = {
            'PID': 'PID',
            'FAMILY ID': 'FAMILY ID',
            'GTID': 'GTID',
            'STUDENT ID': 'STUDENT ID',
            'FIRST NAME': 'FIRST NAME',
            'LAST NAME': 'LAST NAME',
            'GRADE': 'GRADE',
            'STUDENT EMAIL': 'STUDENT EMAIL',
            'HOLD SHIPMENTS FOR ADDRESS': 'HOLD SHIPMENTS FOR ADDRESS',
            'HOLD SHIPMENTS COLLECTIONS': 'HOLD SHIPMENTS COLLECTIONS',
            'RELEASE SHIPMENTS COLLECTIONS CLOSED': 'RELEASE SHIPMENTS COLLECTIONS CLOSED',
            'COMPUTER NEEDED': 'COMPUTER NEEDED',
            'ALREADY HAVE GCA COMPUTER': 'ALREADY HAVE GCA COMPUTER',
            'PRINTER NEEDED': 'PRINTER NEEDED',
            'ALREADY HAVE GCA PRINTER': 'ALREADY HAVE GCA PRINTER',
            'FREE AND REDUCED': 'FREE AND REDUCED',
            'HOTSPOT REQUESTED': 'HOTSPOT REQUESTED',
            'ISP START': 'ISP START',
            'ISP END': 'ISP END',
            'LEARNING COACH FN': 'LEARNING COACH FN',
            'LEARNING COACH LN': 'LEARNING COACH LN',
            'LEARNING COACH EMAIL': 'LEARNING COACH EMAIL',
            'DOB': 'DOB',
            'GENDER': 'GENDER',
            'START DATE': 'START DATE',
            'END DATE': 'END DATE',
            'WD CODE': 'WD CODE',
            'WD PROCESSING DATE': 'WD PROCESSING DATE',
            'ETHNICITY': 'ETHNICITY',
            'MKV': 'MKV',
            'EL': 'EL',
            # '504': '504',
            'SPED': 'SPED',
            'LIFE': 'LIFE',
            'DE': 'DE',
            'GUARDIAN1_FIRSTNAME': 'GUARDIAN1_FIRSTNAME',
            'GUARDIAN1_LASTNAME': 'GUARDIAN1_LASTNAME',
            'GUARDIAN1_PID': 'GUARDIAN1_PID',
            'EMAIL_PRIMARY': 'EMAIL_PRIMARY',
            'GUARDIAN2_FIRSTNAME': 'GUARDIAN2_FIRSTNAME',
            'GUARDIAN2_LASTNAME': 'GUARDIAN2_LASTNAME',
            'GUARDIAN2_PID': 'GUARDIAN2_PID',
            'PRIMARY_PHONE': 'PRIMARY_PHONE',
            'ADDRESS': 'ADDRESS',
            'UnitInfo': 'Unit_Info',
            'CITY': 'CITY',
            'STATE': 'STATE',
            'ZIPCODE': 'ZIPCODE',
            'FEL': 'FEL',
            'FEL EMAIL': 'FEL EMAIL',
            'AT?': 'AT?',
            'ATSpec Teacher': 'ATSpec Teacher',
            'AT EMAIL': 'AT EMAIL',
            'TSL': 'TSL',
            'TSL_Email': 'TSL_Email',
            'AT WIN START': 'AT WIN START',
            'AT WIN END': 'AT WIN END',
            'CTAE WIN START': 'CTAE WIN START',
            'CTAE WIN END': 'CTAE WIN END',
            'DE WIN START': 'DE WIN START',
            'DE WIN END': 'DE WIN END',
            'TS CB START': 'TS CB START',
            'TS CB END': 'TS CB END',
            'TS WIN START': 'TS WIN START',
            'TS WIN END': 'TS WIN END',
            'ZOOMY START': 'ZOOMY START',
            'ZOOMY END': 'ZOOMY END',
            'Duplicate Permitted': 'Duplicate Permitted',
            'Registering?': 'Registering?',
            'App Status': 'App Status',
            'Sy 22-23 Status': 'Sy 22-23 Status',
            'Equipment Return Method': 'Equipment Return Method',
            'REASON AT WINDOWS': 'REASON AT WINDOWS',
            'AT LARGE MONITOR START': 'AT LARGE MONITOR START',
            'AT LARGE MONITOR END': 'AT LARGE MONITOR END'
            # 'Support Services':'Support Services'
        }

        df['FAMILY ID'] = df['FAMILY ID'].fillna(0).astype(int)
        df['GTID'] = df['GTID'].fillna(0).astype(int)

        duplicate_count = df["PID"].duplicated().sum()
        duplicate_rows = df.loc[df["PID"].duplicated(keep='first'), :]
        duplicate_write = df.loc[df["PID"].duplicated(keep=False), :]
        df = df.drop_duplicates(subset='PID', keep=False)

        # df = df.iloc[:, :df.columns.get_loc('HOLD SHIPMENTS FOR ADDRESS') + 1]

        df2 = df.rename(columns=column_mapping)

        if duplicate_count > 0:
            duplicate_write.to_excel(output)
            duplicate_rows = duplicate_rows.iloc[:, :duplicate_rows.columns.get_loc('HOLD SHIPMENTS FOR ADDRESS') + 1]
            duplicate_rows['HOLD SHIPMENTS FOR ADDRESS'] = duplicate_rows['HOLD SHIPMENTS FOR ADDRESS'].fillna(strtoday)
            duplicate_rows.rename(columns=column_mapping)
            df2 = df2.append(duplicate_rows, ignore_index=True)

        print('starting update to SQL')

        gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
        gcaAssetMGMT.df_to_sql(df2, 'stage_studentdata_test')
        gcaAssetMGMT.call('CALL pers_uspupdatestudentdata')
        gcaAssetMGMT.call('CALL asset_uspreassigndevs2youngestest(')
        gcaAssetMGMT.call('CALL asset_uspcbreassign2esfromwd')
        gcaAssetMGMT.call('CALL asset_uspes_sib_kitdist')

        toc = time.time()
        print('Done in {:.4f} seconds'.format(toc - tic))

        today = date.today()
        Date = today
        #
        # fileName_absolute = os.path.basename(student_fileName_relative)
        # new_name = str(Date) + '-' + "nuStudentSheet.csv"
        # shutil.move(student_fileName_relative, 'Z:/Historical/' + new_name)
        # print(new_name + ' moved to Historical Folder.')
