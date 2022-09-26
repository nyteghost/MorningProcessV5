import requests
import pandas as pd
from connectpyse.service import ticket_notes_api, ticket_note, ticket, tickets_api
from connectpyse.schedule import schedule_entries_api, schedule_entry
from datetime import date
from mpConfigs.doorKey import config
from mpConfigs.dbConfig import dbConnect
from _msGraph.auth import getAuth

today = date.today()
strtoday = str(today)
# importlog = setup_logger('BlockUser', log_location + "\\" + "MSGraphAPI" + "\\" + 'ForceBlockUser {}.log'.format(strtoday))
# importlog.info('Starting blockUser')

gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
conn = gcaAssetMGMT.connection()


# Dictionary class
class my_dictionary(dict):
    # __init__ function
    def __init__(self):
        super().__init__()
        self = dict()

    def add(self, key, value):
        self[key] = value


query = f"CALL rep_uspNewStaffTerms"
staffTopsBlock = pd.read_sql(query, conn)
cwURL = 'https://api-na.myconnectwise.net/v2021_2/apis/3.0/'
ENDPOINT = 'https://graph.microsoft.com/v1.0'

# ### Create list of staff members that are being disabled
# list_a=[]
# for i in staffTopsBlock['UserPrincipalName']:
#         list_a.append(i)
# list_a_joined= "\n".join(list_a)


#Create dictionary of staff members and term date that are being disabled
dict_a = my_dictionary()
for u, d in zip(staffTopsBlock.UserPrincipalName, staffTopsBlock.EndDate):
    d = str(d)
    dict_a.add(u, d)

# Variables
# TENANT_ID = config['tenant_id']
# CLIENT_ID = config['client_id']
# AUTHORITY = 'https://login.microsoftonline.com/' + TENANT_ID
#
# SCOPES = [
#     'Files.ReadWrite.All',
#     'Sites.ReadWrite.All',
#     'User.Read',
#     'User.ReadBasic.All'
# ]
#
# cache = msal.SerializableTokenCache()
#
# if os.path.exists('token_cache.bin'):
#     cache.deserialize(open('token_cache.bin', 'r').read())
#
# atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)
#
# app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
#
# accounts = app.get_accounts()
# result = None
# if len(accounts) > 0:
#     result = app.acquire_token_silent(SCOPES, account=accounts[0])
#
# if result is None:
#     flow = app.initiate_device_flow(scopes=SCOPES)
#     if 'user_code' not in flow:
#         raise Exception('Failed to create device flow')
#
#     print(flow['message'])
#
#     result = app.acquire_token_by_device_flow(flow)

result = getAuth('graph')

budata = """{"accountEnabled": 'False'}}"""
buheaders = {'Accept': 'application/json', 'Authorization': 'Bearer ' + result['access_token'],
             'Content-Type': 'application/json'}
for u, d in dict(dict_a).items():
    user = u

    blockl = requests.patch(f'{ENDPOINT}/users/{user}', headers=buheaders, data=budata)

    if blockl.status_code == 404:
        print(u + " not found in portal.")
        # importlog.info(u + " not found in portal.")
        del dict_a[u]
    else:
        print(user)
        ubresult = requests.get(f'{ENDPOINT}/users/{user}?$select=accountEnabled',
                                headers={'Authorization': 'Bearer ' + result['access_token']})
        ubresult.raise_for_status()
        print(blockl.text)
        print(ubresult.text)

print(dict_a)
dict_a_joined = '\n'.join(' TERMED '.join((key, val)) for (key, val) in dict_a.items())

# Creation of ticket
company_id = 22482  # the id of the company
board_name = "GCA Support"  # the id of the board
board_id = 31
summary = 'GCA termed Staff Block List'
resources = 'AMaag'  # should match a cw username
url = config['cwAPI']['web']
auth = config["cwAUTH"]
newTicket = ticket.Ticket(json_dict={
    "company": {"id": "22482"},
    "board": {"name": "GCA Support"},
    "summary": summary,
    "resources": resources})

ct = tickets_api.TicketsAPI(url=url, auth=auth)
new_ticket = ct.create_ticket(newTicket)
tID = new_ticket.id
print(tID)
# importlog.info(tID)
# importlog.info(dict_a_joined)

# Assigns ticket
itID = int(tID)
assign_ticket = schedule_entries_api.ScheduleEntriesAPI(url=url, auth=auth)
assigned = schedule_entry.ScheduleEntry(
    {"objectId": itID, "member": {"identifier": "AMaag"}, "type": {"identifier": "S"}, "ownerFlag": True})
assign_ticket.create_schedule_entry(assigned)

# Create a ticket note
ticket_notes = ticket_notes_api.TicketNotesAPI(url=url, auth=auth, ticket_id=tID)
note = ticket_note.TicketNote(
    {"text": "Please see below for the staff members accounts that have been disabled.\n\n{}".format(dict_a_joined),
     "detailDescriptionFlag": True, "internalAnalysisFlag": True, "externalFlag": False})
ticket_notes.create_ticket_note(note)

#############   DEBUG   ########################################################################################################################
# print(type(data))
# user = "sca_resets@georgiacyber.online"
# if 'access_token' in result:
#     print(user)
#     data = """{"accountEnabled": 'False'}}"""
#     headers= {'Accept': 'application/json', 'Authorization': 'Bearer '+result['access_token'], 'Content-Type': 'application/json'}
#     blockl = requests.patch(f'{ENDPOINT}/users/{user}', headers=headers, data=data)
#     result = requests.get(f'{ENDPOINT}/users/{user}?$select=accountEnabled', headers={'Authorization': 'Bearer ' + result['access_token']})
#     result.raise_for_status()
#     print(blockl.text)
#     print(result.text)
# else:
#     raise Exception('no access token in result')
