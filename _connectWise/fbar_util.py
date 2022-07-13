import sys, os
import doorKey
from doorKey import cwlogin
import requests
from requests.adapters import HTTPAdapter, Retry
import json
import pickle
import time
import getpass
from pyotp import *

retry_strategy = Retry(
    total=10,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


### Config
config=doorKey.tangerine()
tokenHeader = config['cwaHeader']
cwURL = config['cwAPI']['web']
cwAURL = 'https://sca-atl.hostedrmm.com/cwa/api/v1/'

### fBAR folder location
prefix = r"C:\Users"
localuser = getpass.getuser()
suffix= r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\fBAR"
pdFolder = prefix + "\\"+ localuser + suffix


def fbar_cleanUp():
    for f in os.listdir(pdFolder):
        print(f)
        os.remove(os.path.join(pdFolder, f))

### Create an easy dictionay class
class my_dictionary(dict):
    # __init__ function
    def __init__(self):
        self = dict()
    # Function to add key:value
    def add(self, key, value):
        self[key] = value


def getToken():
    loginCRED = cwlogin()
    api_request = cwAURL+'/'+'apitoken'
    response = requests.post(url=api_request, headers=tokenHeader,json=loginCRED)
    dict = response.json()
    accessToken = dict.get('AccessToken')
    data=[]
    data.append(accessToken)
    # file = open('token','wb')
    # pickle.dump(data, file)
    # file.close()
    print('Token Generated.')
    if response.status_code != 200:
        print(api_request)
        print(response.status_code )
        print(response.text)
        pass
    return(accessToken)


def refreshToken():
    api_request = cwAURL+'/'+'apitoken/refresh'
    file = open('token', 'rb')
    data = pickle.load(file)
    file.close()
    for i in data:
        token = i
    # print("old token was",i)
    response = requests.post(url=api_request, headers=tokenHeader,json=token)
    data=[]
    dict = response.json()
    accessToken = dict.get('AccessToken')
    data.append(accessToken)
    file = open('token','wb')
    pickle.dump(data, file)
    file.close()
    if response.status_code != 200:
        print(api_request)
        print(response.status_code )
        print(response.text)
        pass
    return response


def getSpecificComputer(computerName,authToken,compDICT=''):
    cwaGetHeader= getcwaHEADER(authToken)
    api_request = cwAURL+'/'+'Computers?condition=ComputerName ="{computer}"'.format(computer=computerName)
    response = http.get(url=api_request, headers=cwaGetHeader)
    rt = response.text
    try:
        res = json.loads(rt)
    except Exception as e:
        print(e)
        getToken()
        time.sleep(5)
        res = json.loads(rt)
    if compDICT ==1:
        sn_list=[]
        comp_dict = my_dictionary()
        comp_empty_dict = my_dictionary()
        if bool(res):
            emptycomp='Available'
        else:
            emptycomp='NA'
        comp_empty_dict.add(computerName,emptycomp)
        for i in res:
            computerName=i['ComputerName']
            serialNumber=i['SerialNumber']
            comp_dict.add(computerName,serialNumber)
        if response.status_code != 200:
            print(api_request)
            print(response.status_code )
            print(response.text)
            pass
        if __name__ == "__main__":
            print(comp_dict)
        return(comp_dict,comp_empty_dict)
    else:
        for i in res:
            print(i)
            computerName=i['ComputerName']
            serialNumber=i['SerialNumber']

def getcwaHEADER(authToken):
        # file = open('token', 'rb')
        # data = pickle.load(file)
        # file.close()
        # for i in data:
        #        i
        token = "Bearer "+ authToken
        cwaGetHeader = {
                "Authorization":token,
                'clientId':config['cwaHeader']['clientID'],
                "Content-Type":"application/json"
                }
        return(cwaGetHeader)

