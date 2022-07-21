from csv import excel
from distutils.log import error
import getpass
import time
import win32com.client
import os, sys
import logging
import os, sys
import pythoncom
import gc
import xlwings as xw
import threading
from mpConfigs.util_lib import check

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)


logging.basicConfig(
    filename=r'C:\Users\SCA\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\logs\refresh.log',
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    encoding='utf-8',
    filemode='a',
    level=logging.DEBUG)

# Creating an object
refreshlog = logging.getLogger('refresh')

# Setting the threshold of logger to DEBUG
refreshlog.setLevel(logging.DEBUG)


class XwApp(xw.App):
    def __enter__(self, *args, **kwargs):
        return super(*args, **kwargs)

    def __exit__(self, *args):
        for book in self.books:
            try:
                book.close()
            except:
                pass
        self.kill()


def refresh(filename=''):
    check()
    prefix = r"C:\Users" + '\\'
    localuser = getpass.getuser()
    suffix = r"\Southeastern Computer Associates, LLC"
    if filename == 'staff':
        file = r'\GCA Deployment - Documents\Database\GCA Report Requests\Staff Asset Assignments.xlsx'
        savefilename = 'Staff Asset Assignments.xlsx'
    elif filename == 'student':
        file = r'\GCA Deployment - Documents\Database\GCA Report Requests\Student Asset Assignments.xlsx'
        savefilename = 'Student Asset Assignments.xlsx'
    elif filename == 'inventory':
        file = r'\SCA Warehouse - SCA Warehouse Operations\Warehouse Docs\Inventory Report.xlsx'
        savefilename = 'Inventory Report.xlsx'
    elif filename == 'collections':
        file = r'\GCA Deployment - Documents\Database\GCA Report Requests\Collections Data - SCA ON GOING.xlsx'
        savefilename = 'Collections Data - SCA ON GOING.xlsx'
    elif filename == 'asap':
        file = r'\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Post Pickup Summary.xlsx'
        savefilename = 'ASAP Post Pickup Summary.xlsx'
    elif filename == 'ccm':
        file = r'\SCA Warehouse - SCA Warehouse Operations\Cloud CM Solutions\Inventory Reports\CURRENT - CCM Inventory Report.xlsx'
        savefilename = 'CURRENT - CCM Inventory Report.xlsx'
    elif filename == 'cbenroll':
        file = r'\GCA Deployment - Documents\Database\GCA Report Requests\Deployed CB Enrollment Issues.xlsx'
        savefilename = 'Deployed CB Enrollment Issues.xlsx'
    else:
        print("Wrong File Entered")

    fileName = prefix + localuser + suffix + file
    print(fileName)

    savelocation = r'C:\\Users\\SCA\\Desktop\\Excel' + '{savefilename}'

    try:
        wb = xw.Book(fileName)
        time.sleep(10)
        wb.api.Connections(1).OLEDBConnection.BackgroundQuery = False
        print(f"Refreshing {savefilename}")
        wb.api.RefreshAll()
        print(f"Finished refreshing {savefilename}")
        wb.api.Connections(1).OLEDBConnection.BackgroundQuery = True
        wb.save()
        time.sleep(10)
        xl = xw.apps.active.api
        xl.Quit()
    except Exception as e:
        refreshlog.error(f'{filename} refresh failed due to {e}')
        raise Exception(f'{filename} refresh failed due to {e}')
    else:
        refreshlog.info(f'{filename} refresh success.')


def lastModified(suffix=''):
    prefix = r"C:\Users"
    localuser = getpass.getuser()
    inventorySuffix = r"\Southeastern Computer Associates, LLC\SCA Warehouse - SCA Warehouse Operations\Warehouse Docs\Inventory Report.xlsx"
    studentAssetsuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\Student Asset Assignments.xlsx"
    staffAssetsuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\Staff Asset Assignments.xlsx"
    collectionsSuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\Collections Data - SCA ON GOING.xlsx"
    asapSuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Post Pickup Summary.xlsx"
    ccmInventorySuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\ASAP Post Pickup Summary.xlsx"
    cbenrollissueSuffix = r"GCA Deployment - Documents\Database\GCA Report Requests\Deployed CB Enrollment Issues.xlsx"
    studentImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Student Data.xlsx"
    staffImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT GCA Staff Data.xlsx"
    gopherImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT - Gopher Chrome Devices.xlsx"
    collectionsImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT - Collections Data.xlsx"
    claimImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\CURRENT - claim_submission_summary.xlsx"
    dailyUPSImport = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\UPSShipmentsDaily.xlsx"
    cbenrollissueSuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\GCA Report Requests\Deployed CB Enrollment Issues.xlsx"
    if suffix == 'inventory':
        fileName = 'Inventory Report'
        path = inventoryFile = prefix + "\\" + localuser + inventorySuffix
    elif suffix == 'student':
        fileName = 'Student Asset Assignments'
        path = studentAssetFile = prefix + "\\" + localuser + studentAssetsuffix
    elif suffix == 'staff':
        fileName = 'Staff Asset Assignments'
        path = staffAssetFile = prefix + "\\" + localuser + staffAssetsuffix
    elif suffix == 'collections':
        fileName = 'Collections Data'
        path = collectionsFile = prefix + "\\" + localuser + collectionsSuffix
    elif suffix == 'asap':
        fileName = 'ASAP Post Pickup'
        path = asapSuffixFile = prefix + "\\" + localuser + asapSuffix
    elif suffix == 'ccmInventory':
        fileName = 'CURRENT - CCM Inventory Report'
        path = ccmInventoryFile = prefix + "\\" + localuser + ccmInventorySuffix
    elif suffix == 'cbenroll':
        fileName = 'Deployed CB Enrollment Issues.xlsx'
        path = cbenrollissue = prefix + "\\" + localuser + cbenrollissueSuffix

    # Get file's Last modification time stamp only in terms of seconds since epoch
    modTimesinceEpoc = os.path.getmtime(path)
    # Convert seconds since epoch to readable timestamp
    modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modTimesinceEpoc))
    print(fileName, "was last Modified: ", modificationTime)
    refreshlog.info(fileName + " was last Modified: " + modificationTime)


def refreshModifiedcheck():
    lastModified('inventory')
    lastModified('student')
    lastModified('staff')
    lastModified('collections')
    lastModified('asap')
    lastModified('ccmInventory')
    lastModified('cbenroll')


def refreshAll():
    try:
        refresh(filename='inventory')
    except Exception as e:
        refreshlog.info(f'There was a problem with refreshing the inventory;\n{e}')

    try:
        refresh(filename='student')
    except Exception as e:
        refreshlog.info(f'There was a problem with refreshAll the Student File;\n{e}')

    try:
        refresh(filename='staff')
    except Exception as e:
        refreshlog.info(f'here was a problem with refreshAll the Staff File;\n{e}')

    try:
        refresh(filename='collections')
    except Exception as e:
        refreshlog.info(f'There was a problem with refreshAll the Collections File;\n{e}')

    try:
        refresh(filename='asap')
    except Exception as e:
        refreshlog.info(f'There was a problem with refreshAll the ASAP Post File;\n{e}')

    try:
        refresh(filename='ccm')
    except Exception as e:
        refreshlog.info(f'There was a problem with refreshAll the CCM Inventory File;\n{e}')


if __name__ == "__main__":
    # refresh('staff')
    # refresh('collections')
    refreshAll()
    # t = threading.Thread(target=refresh('collections'))
    # t.start()