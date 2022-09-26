import glob
import shutil
import getpass
import os
import pandas as pd
import psutil
import time

prefix = r"C:\Users"
localuser = getpass.getuser()
student_sheet = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Student Data.xlsx"
student_sheetcsv = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Student Data.csv"
student_sheet_fromCSV = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Student Data(fromCSV).xlsx"
staff_sheet = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Staff Data.xlsx"
staff_sheetcsv = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Staff Data.csv"
collections = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT - Collections Data.xlsx"
log_folder = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\logs"

excel_relative_file_path = prefix + "\\" + localuser + student_sheet
staff_excel_relative_file_path = prefix + "\\" + localuser + staff_sheet
nuStudent_excel_relative_file_path = prefix + "\\" + localuser + student_sheet_fromCSV
staff_csv_relative_file_path = prefix + "\\" + localuser + staff_sheetcsv
student_csv_relative_file_path = prefix + "\\" + localuser + student_sheet_fromCSV

collections_excel_relative_file_path = prefix + "\\" + localuser + collections
log_location = prefix + "\\" + localuser + log_folder + "\\"
filenames = [os.path.join(log_location, f) for f in os.listdir(log_location)]


# File exists check
def apple():
    for fileName_relative in glob.glob('G:/Shared drives/SCA - Data & Logistics/Daily Data Sets/*Student*',
                                       recursive=True):
        fileName_absolute = os.path.basename(fileName_relative)
        print(fileName_absolute)
        return "Exists"
    return None


def applejack():
    for nuStudent_fileName_relative in glob.glob('Z:/*SCA_21-22_Final*', recursive=True):
        # nuStudent_fileName_absolute = os.path.basename(nuStudent_fileName_relative)
        # read_file = pd.read_csv(nuStudent_fileName_relative)
        # read_file.to_excel(nuStudent_excel_relative_file_path, index = None, header=True)
        # shutil.copy(nuStudent_fileName_relative, student_sheetcsv)
        # print(nuStudent_excel_relative_file_path)
        return "Exists"
    return None


def banana():
    for staff_fileName_relative in glob.glob('Z:/*Staff*', recursive=True):
        print('There is a staff sheet.')
        staff_fileName_absolute = os.path.basename(staff_fileName_relative)
        read_file = pd.read_csv(staff_fileName_relative)
        read_file.to_excel(staff_excel_relative_file_path, index=None, header=True)
        shutil.copy(staff_fileName_relative, staff_csv_relative_file_path)
        print(staff_excel_relative_file_path)
        return "Exists"
    return None


def cherry():
    for collections_fileName_relative in glob.glob('Z:/*Collections*', recursive=True):
        print('Found collections')
        collections_fileName_absolute = os.path.basename(collections_fileName_relative)
        print(collections_fileName_relative)
        time.sleep(20)
        read_file = pd.read_csv(collections_fileName_relative)
        read_file.to_excel(collections_excel_relative_file_path, index=None, header=True)
        return "Exists"
    return None


def cleanUp():
    suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database"
    pdFolder = prefix + "\\" + localuser + suffix

    for f in os.listdir(pdFolder):
        if f is not None:
            print(f)
            os.remove(os.path.join(pdFolder, f))


def checkIfProcessRunning():
    """
    Check if there is any running process that contains the given name processName.
    """
    for proc in psutil.process_iter():  # Iterate over the all the running process
        try:
            # Check if process name contains the given name string.
            if 'excel'.lower() in proc.name().lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


# For Refresher
def close():
    try:
        os.system('TASKKILL /F /IM excel.exe')
    except Exception as e:
        print(e)


def check():
    wt = 0
    while wt < 2:
        if checkIfProcessRunning():
            print(
                'Previous Excel Refresh still running. I am going to go ahead and wait 15 seconds to make sure closed.')
            time.sleep(15)
            wt += 1
            print(wt)
        else:
            print('No excel process was running')
            break
    if wt == 2:
        close()


