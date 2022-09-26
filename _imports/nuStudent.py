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
from mpConfigs import dbConnect
from _msGraph.teamSend import TeamsChat
import better_exceptions
from deepdiff import DeepDiff
from pprint import pprint
from mpConfigs import filereplace

# better_exceptions.MAX_LENGTH = None
# better_exceptions.hook()

pd.set_option('display.max_rows', 100)

today = date.today()
strtoday = str(today)
Date = today

noHyphenDate = str(Date).replace('-', '')
print(noHyphenDate)

logger.disable("my_library")
logger.add(f"..\\..\\logs\\nuStudent {Date}.log", rotation="500 MB")


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
list_of_column_names_from_dict = []

# Add in new columns from the csv file
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
            'AT Specialist': 'at specialist',
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
            'AT LARGE MONITOR END': 'AT LARGE MONITOR END',
            # 'Support Services':'Support Services'
            'CSLCstartdate': 'cslcstartdate',
            'CSLCenddate': 'cslcenddate',
        }


for student_fileName_relative in glob.glob(f'Z:/*SCA_22-23_Final*', recursive=True):
    if student_fileName_relative is None:
        print('There was not a New Student Data sheet in Z Drive.')
        raise Exception("No nuStudentSheetFound")
    else:
        print('There is a student sheet.')
        filereplace(student_fileName_relative, ",", ";")
        filereplace(student_fileName_relative, ";com", ".com")
        f = io.StringIO()
        with redirect_stderr(f):
            with open(student_fileName_relative, 'r') as file:
                reader = csv.reader(file, quotechar='"', delimiter='|')
                # if the first line is header
                header = 0
                for row in reader:
                    if header == 0:
                        header = 1
                        print(len(row))
                        if len(row) > 69:
                            reportError = TeamsChat('portal_posse')
                        list_of_column_names = row   # reportError.send(f'A extra column has been found.', 'Nathan')
                        error_liens.append("|".join(row))
                        good_lines.append("|".join(row))
                        continue
                    # checking if number of columns is not equal to 3 (bad lines)
                    if len(row) > 72:
                        error_liens.append("|".join(row))
                        good_lines.append("|".join(row[:9]) + strtoday)
                    else:
                        good_lines.append("|".join(row))

            for k, v in column_mapping.items():
                list_of_column_names_from_dict.append(k)

            l1 = list_of_column_names
            l2 = list_of_column_names_from_dict
            l1.sort()
            l2.sort()
            print(l1)
            print(l2)

            # print()
            # res = [x for x in l1 + l2 if x not in l1 or x not in l2]
            # print()

            ddiff = DeepDiff(l1, l2, ignore_order=True)
            print()
            print('Columns found in Student Sheet not in Column Mapping.')
            pprint(ddiff['iterable_item_removed'])
            print()
            print('Columns found in Column Mapping not found In Student Sheet')
            pprint(ddiff['iterable_item_added'])

            # write the error list to error file
            errorWrite = open(extraColumnError, "w")
            errorWrite.write("\n".join(error_liens))
            errorWrite.close()

            goodWrite = open(goodStuff, "w")
            goodWrite.write("\n".join(good_lines))
            goodWrite.close()

        df = pd.DataFrame([sub.split("|") for sub in good_lines])
        df.columns = df.iloc[0]
        df = df[1:]
        print(df)

        # df = pd.read_csv(
        #     goodStuff, on_bad_lines='skip', index_col=False, low_memory=False)
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

        df = pd.read_excel(student_excel_relative_file_path)

        df['PID'] - df['PID'].fillna(0).astype(int)
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

        # Need to put in lower case for the df_to_sql to work
        dtype = {'504': 'object',
                 'address': 'object',
                 'at email': 'object',
                 'at large monitor end': 'datetime64[ns]',
                 'at large monitor start': 'datetime64[ns]',
                 'at specialist': 'object',
                 'at win end': 'datetime64[ns]',
                 'at win start': 'datetime64[ns]',
                 'at?': 'object',
                 'city': 'object',
                 'cslcstartdate': 'datetime64[ns]',
                 'cslcenddate': 'datetime64[ns]',
                 'computer needed': 'object',
                 'ctae win end': 'datetime64[ns]',
                 'ctae win start': 'datetime64[ns]',
                 'de': 'object',
                 'de win end': 'datetime64[ns]',
                 'de win start': 'datetime64[ns]',
                 'dob': 'object',
                 'duplicate permitted': 'object',
                 'el': 'object',
                 'email_primary': 'object',
                 'end date': 'datetime64[ns]',
                 'ethnicity': 'object',
                 'family id': 'object',
                 'fel': 'object',
                 'fel email': 'object',
                 'first name': 'object',
                 'free and reduced': 'object',
                 'gender': 'object',
                 'grade': 'object',
                 'gtid': 'object',
                 'guardian1_firstname': 'object',
                 'guardian1_lastname': 'object',
                 'guardian1_pid': 'int64',
                 'guardian2_firstname': 'object',
                 'guardian2_lastname': 'object',
                 'guardian2_pid': 'int64',
                 'hold shipments collections': 'datetime64[ns]',
                 'hold shipments for address': 'datetime64[ns]',
                 'hotspot requested': 'object',
                 'isp end': 'datetime64[ns]',
                 'isp start': 'datetime64[ns]',
                 'last name': 'object',
                 'learning coach email': 'object',
                 'learning coach fn': 'object',
                 'learning coach ln': 'object',
                 'life': 'object',
                 'mkv': 'object',
                 'pid': 'int64',
                 'primary_phone': 'object',
                 'printer needed': 'object',
                 'reason at windows': 'object',
                 'release shipments collections closed': 'datetime64[ns]',
                 'sped': 'object',
                 'start date': 'datetime64[ns]',
                 'state': 'object',
                 'student email': 'object',
                 'student id': 'int64',
                 'ts cb end': 'datetime64[ns]',
                 'ts cb start': 'datetime64[ns]',
                 'ts win end': 'datetime64[ns]',
                 'ts win start': 'datetime64[ns]',
                 'tsl': 'object',
                 'tsl_email': 'object',
                 'unit_info': 'object',
                 'wd code': 'object',
                 'wd processing date': 'datetime64[ns]',
                 'zipcode': 'object',
                 'zoomy end': 'datetime64[ns]',
                 'zoomy start': 'datetime64[ns]'
                 }


        # df2 = df2.fillna(0)

        df2.columns = df2.columns.str.lower()
        for k, v in dtype.items():
            df2[k] = df2[k].astype(v, errors='ignore')

        df2['dob'] = pd.to_datetime(df2['dob'], format='%#d/%#m/%Y', infer_datetime_format=True)  # format=f'{your_date_style}'

        # df2 = df2.drop(columns=['unit_info'])
        # df2 = df2.loc[:, ~df2.columns.str.contains('^Unnamed')]
        gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
        gcaAssetMGMT.df_to_sql(df2, 'stage_studentdata')

        try:
            tryCall1 = gcaAssetMGMT.call('pers_uspupdatestudentdata')
            tryCall2 = gcaAssetMGMT.call('asset_uspreassigndevs2youngestest')
            tryCall3 = gcaAssetMGMT.call('asset_uspcbreassign2esfromwd')
            tryCall4 = gcaAssetMGMT.call('asset_uspes_sib_kitdist')
        except Exception as e:
            raise Exception(e)
        else:
            if tryCall1 & tryCall2 & tryCall3 & tryCall4:
                fileName_absolute = os.path.basename(student_fileName_relative)
                new_name = "r" + str(Date) + 'r-' + fileName_absolute
                shutil.move(student_fileName_relative, 'Z:/Historical/' + new_name)
                print(new_name + ' moved to Historical Folder.')

        toc = time.time()
        print('Done in {:.4f} seconds'.format(toc - tic))

        today = date.today()
        Date = today


