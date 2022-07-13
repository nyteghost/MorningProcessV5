import requests
import msal
import atexit
import pandas as pd
from datetime import date
import os
from mpConfigs.doorKey import config
from mpConfigs.dbConfig import dbConnect
from mpConfigs.logger_setup import setup_logger,log_location

today = date.today()
strtoday = str(today)

# importlog = setup_logger('ForcePassChange' ,log_location+"\\"+"MSGraphAPI"+"\\"+'ForcePassChange {}.log'.format(strtoday))

gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
conn = gcaAssetMGMT.connection()

# importlog.info("Starting forcePassChange process.")

query = f"CALL GCAAssetMGMT_2_0.Ship.uspStaffKitDeployPWsToReset"
staffTops = pd.read_sql(query, conn)

TENANT_ID = config['tenant_id']
CLIENT_ID = config['client_id']

AUTHORITY = 'https://login.microsoftonline.com/' + TENANT_ID
ENDPOINT = 'https://graph.microsoft.com/v1.0'

SCOPES = [
    'Files.ReadWrite.All',
    'Sites.ReadWrite.All',
    'User.Read',
    'User.ReadBasic.All'
]

cache = msal.SerializableTokenCache()

if os.path.exists('token_cache.bin'):
    cache.deserialize(open('token_cache.bin', 'r').read())

atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)

app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

accounts = app.get_accounts()
result = None
if len(accounts) > 0:
    result = app.acquire_token_silent(SCOPES, account=accounts[0])

if result is None:
    flow = app.initiate_device_flow(scopes=SCOPES)
    if 'user_code' not in flow:
        raise Exception('Failed to create device flow')

    print(flow['message'])

    result = app.acquire_token_by_device_flow(flow)
# importlog.info('Below are the users whom are having the "forceChangePasswordNextSignIn" set to TRUE.')
for i in staffTops['Org_Secondary_Email']:
    user = i
    print(user)
    # importlog.info(user)
    data = """{"passwordProfile": {"forceChangePasswordNextSignIn": 'TRUE'}}"""
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer '+result['access_token'], 'Content-Type': 'application/json'}
    resp = requests.patch(f'{ENDPOINT}/users/{user}', headers=headers, data=data)

    print(resp.status_code)








#############   DEBUG   ########################################################################################################################

# if 'access_token' in result:
#     result = requests.get(f'{ENDPOINT}/users/{user}', headers={'Authorization': 'Bearer ' + result['access_token']})
#     result.raise_for_status()
#     print(result.json())
# else:
#     raise Exception('no access token in result')

# headers = CaseInsensitiveDict()
# headers["Accept"] = "application/json"
# headers["Authorization"] = "Bearer "+result['access_token']
# headers["Content-Type"] = "application/json"