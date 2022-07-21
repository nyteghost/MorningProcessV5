from __future__ import print_function
from connectpyse.service import ticket_notes_api, ticket_note, tickets_api
from connectpyse.system import document_api
from connectpyse.schedule import schedule_entries_api, schedule_entry
import requests
from bs4 import BeautifulSoup
import quopri
import json
from datetime import date, timedelta
import dateutil.parser as dparser
from mpConfigs.dbConfig import dbConnect
from rich import print
import sys, os
from mpConfigs.doorKey import config
import pandas as pd
import time

cwAUTH = config['cwAUTH']
cwURL = config['cwAPI']['web']
# cwDocURL = 'https://cloud.na.myconnectwise.net/v4_6_development/apis/3.0'
# cwAURL = 'https://sca-atl.hostedrmm.com/cwa/api/v1/'
# cwTEURL = config['cwAPI']['web']+'/time/entries'
cwDocumentHeaders = config['cwDocumentHeaders']

# Connection to SQL Database
gcaAssetMGMT = dbConnect("gcaassetmgmt_2_0")
conn = gcaAssetMGMT.connection()

### Lists ###
ticket_number_list = []
doc_list = []
andtheotherdict = {}

### Set up for start = beginning of week
today = date.today()
start = today - timedelta(days=today.weekday())
end = start + timedelta(days=6)

# start = '"2022-04-20"'
begin = start
yesterday = today - timedelta(days=1)
start = "[" + str(yesterday) + "]"

### Get Tickets ###
gt = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
gt.conditions = f'company/identifier="Georgia Cyber Academy" AND status/name != "Closed" AND status/name != "Work Completed" AND _info/dateEntered > {start} AND summary contains "Unconfirmed GA Cyber Pickup"\
                OR company/identifier="Georgia Cyber Academy" AND status/name != "Closed" AND status/name != "Work Completed" AND _info/dateEntered > {start} AND summary contains "Unconfirmed Pickups"'
gt.pageSize = 1000
gt.orderBy = '_info/dateEntered'
gt = gt.get_tickets()
ls = list(gt)

# for i in ls:
#     print(i)
#     print(i._info['dateEntered'])
#     input()

for ticketID in ls:
    thisdict = {}
    thatdict = {}
    ticket_number = ticketID.id
    ticket_summary = ticketID.summary
    tn = ticket_notes_api.TicketNotesAPI(url=cwURL, auth=cwAUTH, ticket_id=ticket_number)
    tn = tn.get_ticket_notes()
    tnlist = list(tn)

    for i in tnlist:
        print(ticket_number)
        note_from_ticket = str(i)
        ticketENTERDATE = ticketID._info['dateEntered']

        ticket_date_from_summary = dparser.parse(ticket_summary, fuzzy=True).date()
        ticket_date_from_summary = str(ticket_date_from_summary)

        if "All Pickups Confirmed" in ticket_summary or "All of the GA Cyber Pickups for" in note_from_ticket:
            try:
                ticket_notes = ticket_notes_api.TicketNotesAPI(url=cwURL, auth=cwAUTH, ticket_id=ticket_number)
                note = ticket_note.TicketNote(
                    {"text": "All pickups have been confirmed. Completed with Python automagically.",
                     "detailDescriptionFlag": True})
                ticket_notes.create_ticket_note(note)
                api = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
                ticket = api.update_ticket(ticket_number, "/status/id", "581")
            except Exception as e:
                print(ticket_number, e)
            continue
        else:
            # print(ticketID,contactNAME,ticketSTATUS,ticketENTERDATE)
            # print("\n",ticket_number)
            o = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
            d = document_api.DocumentAPI(url=cwURL, auth=cwAUTH)
            a_ticket = o.get_ticket_by_id(ticket_number)
            myDocs = d.get_documents(a_ticket)

            for doc in myDocs:
                if doc.fileName.endswith('.eml') and doc.fileName.startswith('Unconfirmed GA Cyber Pickups'):
                    print(doc.id, doc.title)
                    # print(doc)
                    # docTITLE=doc.title
                    # docTITLE=docTITLE.replace(':',"")
                    docID = doc.id
                    doc_list.append(docID)
                    url = cwURL + 'system/documents/{document_id}/download'.format(document_id=docID)
                    response = requests.get(url=url, headers=cwDocumentHeaders)
                    body = quopri.decodestring(response.content)
                    ### Use BS4 to scrape the information from the email###
                    soup = BeautifulSoup(body, "html.parser")

                    rows = []
                    try:
                        for child in soup.find_all('table')[0].children:
                            row = []
                            for td in child:
                                try:
                                    row.append(td.text.replace('\n', ''))
                                except:
                                    continue
                            if len(row) > 0:
                                rows.append(row)
                        for i in rows[1:]:
                            thisdict[i[0]] = i[1]
                    except IndexError as err:
                        if str(err) == 'IndexError: list index out of range':
                            pass

            thatdict["Date " + ticket_date_from_summary] = thisdict
            if any(thatdict.values()) == True:

                that_json_object = json.dumps(thatdict, indent=4)
                print(that_json_object)
                andtheotherdict["Date " + ticket_date_from_summary] = thisdict
                print('Uploading to Database')
                cursor.execute(f"EXEC GCAAssetMGMT_2_0.Ship.uspUpdateUnconfirmedPickups '{that_json_object}';")
                conn.commit()
                conn.close()
                ## Enter Ticket Notes ###

                try:
                    ticket_notes = ticket_notes_api.TicketNotesAPI(url=cwURL, auth=cwAUTH, ticket_id=ticket_number)
                    note = ticket_note.TicketNote(
                        {"text": "Completed with Python automagically.", "detailDescriptionFlag": True})
                    ticket_notes.create_ticket_note(note)
                    api = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
                    ticket = api.update_ticket(ticket_number, "/status/id", "581")
                except Exception as e:
                    print(ticket_number, e)

            else:
                print("No thatdict values found")
                break

# and_the_other_json_object = json.dumps(andtheotherdict, indent = 4)
# print(and_the_other_json_object)

# print('Uploading to Database')
# cursor.execute(f"EXEC GCAAssetMGMT_2_0.Ship.uspUpdateUnconfirmedPickups '{and_the_other_json_object}';")
# conn.commit()
# #print(andtheotherdict)
# conn.close()