import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import time
import getpass
import os
import pandas as pd
from datetime import date
import logging
import ast
from urllib.error import HTTPError
from mpConfigs.dbConfig import dbConnect
from mpConfigs.doorKey import config


# logging.basicConfig(filename='chromeos_device_info.log',level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s:%(message)s',filemode='w')
# logger = logging.getLogger(__name__)

use_loguru = 0

if __name__ == "__main__":
    from loguru import logger

    # logger.add("./logs/ChromeOS_Devices-{time}.log")
    use_loguru = 1

today = date.today()
strtoday = str(today)
gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
conn = gcaAssetMGMT.connection()


# Column Mapping
column_mapping = {
    'kind': '"kind"',
    'etag': 'etag',
    'deviceId': 'deviceId',
    'serialNumber': 'serialNumber',
    'status': 'status',
    'lastSync': 'lastSync',
    'annotatedUser': 'annotatedUser',
    'annotatedAssetId': 'annotatedAssetId',
    'model': 'model',
    'osVersion': 'osVersion',
    'platformVersion': 'platformVersion',
    'firmwareVersion': 'firmwareVersion',
    'macAddress': 'macAddress',
    'bootMode': 'bootMode',
    'lastEnrollmentTime': 'lastEnrollmentTime',
    'orgUnitPath': 'orgUnitPath',
    'orgUnitId': 'orgUnitId',
    'recentUsers': 'recentUsers',
    'ethernetMacAddress': 'ethernetMacAddress',
    'activeTimeRanges': 'activeTimeRanges',
    'tpmVersionInfo': 'tpmVersionInfo',
    'cpuStatusReports': 'cpuStatusReports',
    'systemRamTotal': 'systemRamTotal',
    'systemRamFreeReports': 'systemRamFreeReports',
    'diskVolumeReports': 'diskVolumeReports',
    'lastKnownNetwork': 'lastKnownNetwork',
    'ethernetMacAddress0': 'ethernetMacAddress0',
    'dockMacAddress': 'dockMacAddress',
    'manufactureDate': 'manufactureDate',
    'autoUpdateExpiration': 'autoUpdateExpiration',
    'cpuInfo': 'cpuInfo',
    'annotatedLocation': 'annotatedLocation',
    'notes': 'notes',
    'meid': 'meid'
}
replace_Dict = {
    "{\'": '{"',
    " \'": ' "',
    "\',": '",',
    "\':": '":',
    "\'}": '"}'
}
conversionMapping = {
    "serialNumber": replace_Dict,
    "status": replace_Dict,
    "lastSync": replace_Dict,
    "annotatedUser": replace_Dict,
    "model": replace_Dict,
    "osVersion": replace_Dict,
    "platformVersion": replace_Dict,
    "firmwareVersion": replace_Dict,
    "lastEnrollmentTime": replace_Dict,
    "orgUnitPath": replace_Dict,
    "recentUsers": replace_Dict,
    "ethernetMacAddress": replace_Dict,
    "activeTimeRanges": replace_Dict,
    "tpmVersionInfo": replace_Dict,
    "cpuStatusReports": replace_Dict,
    "systemRamFreeReports": replace_Dict,
    "diskVolumeReports": replace_Dict,
    "lastKnownNetwork": replace_Dict,
    "ethernetMacAddress0": replace_Dict,
    "dockMacAddress": replace_Dict,
    "manufactureDate": replace_Dict,
    "cpuInfo": replace_Dict,
    "notes": replace_Dict
}

dflst = []

### Locations
prefix = r"C:\Users"
localuser = getpass.getuser()
credsuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Sensitive\credentials.json"
tokensuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Sensitive\token.json"
credentials = prefix + "\\" + localuser + credsuffix
gtoken = prefix + "\\" + localuser + tokensuffix

#### SQL VARIABLES
updateAssetquery = r"CALL rep_uspgsuitedevstoupdateassetid"
processquery = r"CALL rep_uspupdategsuitedevstbl"

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/admin.directory.device.chromeos']


def getCredentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(gtoken):
        creds = Credentials.from_authorized_user_file(gtoken, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(config, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(gtoken, 'w') as token:
            token.write(creds.to_json())
    service = build('admin', 'directory_v1', credentials=creds)
    return service


def requestPoint(service, pt):
    # print(service)
    # Call the Admin SDK Directory API
    # print('Getting the first 10 devices in the domain')
    if pt == 'start':
        print('Starting Loop')
        results = service.chromeosdevices().list(projection='FULL', customerId=config['google']['APIClientID'],
                                                 maxResults=200).execute()
    else:
        results = service.chromeosdevices().list(projection='FULL', pageToken=pt,
                                                 customerId=config['google']['APIClientID'], maxResults=200).execute()
    # pprint(results)
    devices = results.get('chromeosdevices', [])
    pageToken = results.get('nextPageToken', [])
    # print(devices)
    if not devices:
        print('No devices in the domain.')
        return False
    else:
        for device in devices:
            df = pd.DataFrame([device], columns=list(column_mapping.keys()))
            # print(df,df.dtypes)
            dflst.append(df)
    if not pageToken:
        return False
    else:
        return pageToken


def updateRequestPoint(service, deviceID, newAsset):
    response = service.chromeosdevices().get(customerId='C037bk70g', deviceId=deviceID).execute()
    print("deviceId :", response['deviceId'])
    try:
        print("annotatedAssetId :", response['annotatedAssetId'])
    except KeyError as e:
        print(e, " not found")
    finally:
        print("serialNumber :", response['serialNumber'])
        print("orgUnitPath :", response['orgUnitPath'])
        print("status :", response['status'])
    try:
        request = service.chromeosdevices().patch(customerId='C037bk70g', deviceId=deviceID, body=newAsset).execute()
    except HTTPError as e:
        print(e)
    try:
        print("New annotatedAssetId :", request['annotatedAssetId'])
    except KeyError as e:
        print(e, " not found")
    return request['annotatedAssetId']


def updateOrgData():
    query = 'CALL isolatedsafety.dbo_uspdevstomovetostudentou'
    result = pd.read_sql(query, conn)
    print(result)
    if not result.empty:
        jsonDFCall = result['JSONs']
        print(jsonDFCall)
        if not jsonDFCall.empty:
            print('Found OU Updates')
            service = getCredentials()
            for n in range(len(result)):
                print(n)
                for chrome_move_json in jsonDFCall:
                    if chrome_move_json:
                        chrome_move_json = ast.literal_eval(chrome_move_json)
                        result = service.chromeosdevices().moveDevicesToOu(customerId=config['google']['APIClientID'],
                                                                           orgUnitPath='/Students',
                                                                           body=chrome_move_json).execute()
                        time.sleep(10)
                    else:
                        break
        else:
            print('No OU updates needed')
    print('Org Data Update Complete')

def updateMain():
    updateAsset = pd.read_sql(updateAssetquery, conn)
    for i in range(len(updateAsset)):
        deviceId = updateAsset.loc[i, 'deviceid']
        newAsset = updateAsset.loc[i, 'newassetid']
        print('\nNew Asset ID :', newAsset)
        service = getCredentials()
        updateRequestPoint(service, deviceId, {"annotatedAssetId": "{}".format(newAsset)})
    print('updateMain Complete')


def gopherMain():
    # startTime = time.time()
    service = getCredentials()
    loops = 0
    pt = 'start'
    while pt:
        loops += 1
        pt = requestPoint(service, pt)
        if loops % 5 == 0:
            print("Page :", loops, " complete.")
        if not pt:
            break
    # print(dflst)
    tempDF = pd.concat(dflst)
    tempDF = tempDF.astype(str).replace(conversionMapping, regex=True).where(pd.notnull(tempDF), None)
    # print(tempDF,tempDF.dtypes)
    # tempDF.to_excel('StudentChromeDevices.xlsx',index=False)

    gcaAssetMGMT.df_to_sql(tempDF, 'rep_gsuitedevices2')
    gcaAssetMGMT.call('rep_uspupdategsuitedevstbl')
    print('Gopher Main Complete')


startTime = time.time()
gopherMain()
updateOrgData()
updateMain()
completeTime = (time.time() - startTime)
print('Completed ChromeOS')
print(int(completeTime / 60), 'minutes', int(completeTime) % 60, 'seconds to complete import.')
