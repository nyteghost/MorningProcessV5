import requests
import pandas as pd
import time
# from alive_progress import alive_bar, alive_it
from requests.adapters import HTTPAdapter, Retry
from usps import USPSApi, Address
import sys
import json
from mpConfigs.doorKey import config
from mpConfigs.dbConfig import dbConnect

# Timer Start
startTime = time.time()

gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
conn = gcaAssetMGMT.connection()


# List/Dict creation
upsrows = []
uspsrows = []
revaluspsrows = []
keyErrorList = []
typeErrorList = []
listedAddress = []
addressDictError = {}
timeStart = 0

# Map columns of DataFrame with SQL Database
column_mapping = {'personid': '"personid"',
                  'address': 'street1',
                  'address_2': '"street2"',
                  'city': '"city"',
                  'state': 'state',
                  'postal_code': 'postalcode',
                  'val_src': 'val_src'}

# Retry Setup
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])

# Loop Setup
clockon = 0
breakvalue = 0


# Validation Functions

def street_name_fix(StreetName):
    replacements = {'RD': 'ROAD',
                    'CIR': 'CIRCLE',
                    'DR': 'DRIVE',
                    'LN': 'LANE',
                    'CT': 'COURT',
                    'PL': 'PLACE',
                    'ST': 'STREET',
                    'BLVD': 'BOULEVARD',
                    'WY': 'WAY',
                    'AVE': 'AVENUE',
                    'OPAS': 'OVERPASS',
                    'TRL': 'TRAIL',
                    'MNR': 'MANOR'}

    StreetName = StreetName.upper().strip().rstrip('.')
    try:
        return '{} {}'.format(' '.join(StreetName.split()[:-1]), replacements[StreetName.split()[-1]])
    except IndexError:
        return (StreetName)
    except KeyError:
        return (StreetName)


def upsValidation(pID, street, street2, city, state, zipCode, listName):
    data = {
        "UPSSecurity": {
            "UsernameToken": {"Username": config['ups']['name'], "Password": config['ups']['password']},
            "ServiceAccessToken": {"AccessLicenseNumber": config['ups']['accesskey']},
        },
        "XAVRequest": {
            "Request": {
                "RequestOption": "1",
                "TransactionReference": {"CustomerContext": ""},
            },
            "MaximumListSize": "10",
            "AddressKeyFormat": {
                "ConsigneeName": "",
                "BuildingName": "",
                "AddressLine": street,
                "PoliticalDivision2": city,
                "PoliticalDivision1": state,
                "PostcodePrimaryLow": zipCode,
                "CountryCode": "US",
            },
        },
    }
    url = "https://onlinetools.ups.com/rest/XAV"
    retries = 1
    success = False
    while not success and retries < 10:
        try:
            print(street)
            response = requests.post(url, json=data, timeout=5)
            success = True
        except Exception as e:
            wait = retries * 5
            print('Error! Waiting %s secs and re-trying...' % wait)
            sys.stdout.flush()
            time.sleep(wait)
            retries += 1
    info = response.json()
    json_dump = json.dumps(info)
    dict_json = json.loads(json_dump)
    # print("##########################################")
    # print(dict_json)
    # print("##########################################")
    if "Candidate" in dict_json["XAVResponse"] and "AddressKeyFormat" in dict_json["XAVResponse"]["Candidate"]:
        xavresquest = data['XAVRequest']['AddressKeyFormat']
        xavresponse = info["XAVResponse"]["Candidate"]['AddressKeyFormat']

        # if street2 ==None:
        #     street = xavresponse['AddressLine']
        #     if type(street) is list:
        #         print(type(street))
        #         street2 = street[1]
        #         street = street[0]
        print(pID, street, street2, city, state, zipCode)
        print("Validation request address: ", xavresquest['AddressLine'], xavresquest['PoliticalDivision2'],
              xavresquest['PoliticalDivision1'], xavresquest['PostcodePrimaryLow'])
        print("Validation result address: ", street, street2, xavresponse['PoliticalDivision2'],
              xavresponse['PoliticalDivision1'], xavresponse['PostcodePrimaryLow'])
        listName.append([pID, xavresponse['AddressLine'], street2, xavresponse['PoliticalDivision2'],
                         xavresponse['PoliticalDivision1'], xavresponse['PostcodePrimaryLow'], 'UPS'])
    else:
        print('No address found with UPS. Checking address with USPS.')
        uspsValidation(pID, street, street2, city, state, zipCode, uspsrows, 'USPS', 1)
        pass
    print('\n')


def uspsValidation(pID, street, street2, city, state, zipCode, listName, valueType, format=''):
    if format == 1:
        k = []
        street = street.upper()
        s = street
        l = street.split()
        for i in l:
            i = street_name_fix(i).lstrip()
            if s.count(i) >= 1 and (i not in k):
                k.append(i)
        street = ' '.join(k)

    address = Address(
        name='',
        address_1=street.strip(),
        address_2=street2,
        city=city.strip(),
        state=state.strip(),
        zipcode=zipCode.strip()
    )
    usps = USPSApi(config['usps']['accesskey'], test=True)
    try:
        validation = usps.validate_address(address)
    except Exception:
        print('Hit exception during Validation')
        pass
    else:
        print('\n')
        print("PersonalID: ", pID)
        print("Address: ", street, street2, city, state, zipCode)
        if 'AddressValidateResponse' in validation.result:
            if 'Error' in validation.result['AddressValidateResponse']['Address']:
                print('Address Not Found')
            else:
                addressVal = validation.result['AddressValidateResponse']['Address']
                if addressVal['Address1'] == "NONE" or addressVal['Address1'] == "-":
                    print(addressVal["Address2"], addressVal["City"], addressVal["State"], addressVal["Zip5"])
                    listName.append(
                        [pID, addressVal["Address2"], None, addressVal["City"], addressVal["State"], addressVal["Zip5"],
                         valueType])

                else:
                    print(addressVal["Address2"], addressVal["Address1"], addressVal["City"], addressVal["State"],
                          addressVal["Zip5"])
                    listName.append(
                        [pID, addressVal["Address2"], addressVal["Address1"], addressVal["City"], addressVal["State"],
                         addressVal["Zip5"], valueType])
        else:
            print('No USPS address found')


def revalidation():
    # SQL Queries
    revalidationQuery = f"SELECT * FROM gcaassetmgmt_2_0.ship_vwaddressesneeding2ndvalidation"
    revalidationData = pd.read_sql(revalidationQuery, conn)
    revalidationData.index = revalidationData.index + 1
    print(revalidationData)

    # Second Execution
    for i in revalidationData.index:
        pID = revalidationData['personid'].loc[i]
        street = revalidationData['address'].loc[i]
        street2 = revalidationData['address_2'].loc[i]
        city = revalidationData['city'].loc[i]
        state = revalidationData['state'].loc[i]
        zipCode = revalidationData['postal_code'].loc[i]

        uspsValidation(pID, street, street2, city, state, zipCode, revaluspsrows, "reval-USPS", 1)
    print(revaluspsrows)


def main():
    # SQL Queries
    shipDataQuery = f"SELECT * FROM gcaassetmgmt_2_0.ship_vwaddressvalidation"
    shipData = pd.read_sql(shipDataQuery, conn)
    shipData.index = shipData.index + 1
    print(shipData)

    # Main Execution
    for i in shipData.index:
        print(i)
        pID = shipData['personid'].loc[i]
        street = shipData['address'].loc[i]
        street2 = shipData['address_2'].loc[i]
        # if street2 != None:
        #     street = street +" "+ street2
        city = shipData['city'].loc[i]
        state = shipData['state'].loc[i]
        zipCode = shipData['postal_code'].loc[i]
        upsValidation(pID, street, street2, city, state, zipCode, upsrows)

    revalidation()
    if upsrows:
        df = pd.DataFrame(upsrows, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(upsrows), None).replace('\.0', '', regex=True)
        print(df)
        gcaAssetMGMT.df_to_sql(df, 'ship_ups_validatedaddress')
    else:
        print("Empty UPS rows found")

    if uspsrows:
        df = pd.DataFrame(uspsrows, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(uspsrows), None).replace('\.0', '', regex=True)
        print(df)
        gcaAssetMGMT.df_to_sql(df, 'ship_ups_validatedaddress')
    else:
        print("Empty USPS row found")

    if revaluspsrows:
        df = pd.DataFrame(revaluspsrows, columns=list(column_mapping.keys())).astype(str).where(pd.notnull(revaluspsrows), None).replace('\.0', '', regex=True)
        print(df)
        gcaAssetMGMT.df_to_sql(df, 'ship_ups_validatedaddress')
    else:
        print("Empty reval-USPS rows.")

    gcaAssetMGMT.call(f"ship_cleanvalidatedaddresses")

    completeTime = (time.time() - startTime)
    print(completeTime)


main()
