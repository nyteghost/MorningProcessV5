import msal
import atexit
import os
from dotenv import load_dotenv
import getpass

load_dotenv('../mpConfigs/config.env')

TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
AUTHORITY = 'https://login.microsoftonline.com/' + TENANT_ID
ENDPOINT = 'https://graph.microsoft.com/v1.0'

tokenLoc = f'C:/Users/{getpass.getuser()}/Southeastern Computer Associates, LLC/GCA Deployment - Documents/Database/Daily Data Sets/Sensitive/tokens/'

teamToken = tokenLoc + 'teams_token_cache.bin'
graphToken = tokenLoc + 'graph_token_cache.bin'


def getAuth(scope=''):
    SCOPES = None
    token = None
    if scope == 'teams':
        SCOPES = [
            'Chat.ReadWrite',
            'ChatMessage.Read',
            'ChatMessage.Send',
            'User.Read'
        ]
        token = teamToken

    elif scope == 'graph':
        SCOPES = [
            'Files.ReadWrite.All',
            'Sites.ReadWrite.All',
            'User.Read',
            'User.ReadBasic.All'
        ]
        token = graphToken

    cache = msal.SerializableTokenCache()

    if os.path.exists(token):
        cache.deserialize(open(token, 'r').read())

    atexit.register(lambda: open(token, 'w').write(cache.serialize()) if cache.has_state_changed else None)

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
    return result
