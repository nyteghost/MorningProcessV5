from numpy import add
from sqlalchemy import create_engine
import pandas as pd
import getpass
import re
from clint.textui import puts, colored, indent
from connectpyse.system import document_api
from connectpyse.service import ticket, ticket_notes_api, ticket_note, ticket, tickets_api
from connectpyse.time import time_sheets_api, time_sheet, time_entries_api, time_entry
from pretty_html_table import build_table
from dateutil import parser
from datetime import date, datetime
# import datetime
import time
import pytz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os, sys
import smtplib, ssl
import json
import urllib
import os, sys

from mpConfigs.doorKey import config

"""
Add in checking for last entry of ticket and compare if D&L member or not.

Compare by latest note vs latest time entry
"""

cwAUTH = config['cwAUTH']
cwURL = config['cwAPI']['web']
cwDocURL = 'https://cloud.na.myconnectwise.net/v4_6_development/apis/3.0'
cwAURL = 'https://sca-atl.hostedrmm.com/cwa/api/v1/'
cwTEURL = config['cwAPI']['web'] + '/time/entries'

# Connection to SQL Database
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 'Server=' + (config['database']['Server']) + ';'
                                                                              'Database=IsolatedSafety;'
                                                                              'UID=' + (config['database']['UID']) + ';'
                                                                                                                     'PWD=' + (
                                 config['database']['PWD']) + ';')
conn = create_engine(f"mssql+pyodbc:///?odbc_connect={params}&autocommit=true")

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Downloads\srboard.csv"
outstanding_list = prefix + "\\" + localuser + suffix

## Used for Address-ReturnCheck script to find Tickets for GCA with Scheduled as identifier
# df = pd.DataFrame(columns=["Ticket #","Contact","Status","Creation Date"]) #Create DataFrame
# df2 = pd.DataFrame(columns=["Ticket #","Contact","Status","Creation Date"]) #Create re-Opened DataFrame
columns = ["Ticket #", "Contact", "Status", "Creation Date"]
data = []  # Empty List used for dataframe
reOpenedList = []
gt = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
gt.conditions = 'company/identifier="Georgia Cyber Academy" AND summary contains "Request" AND status/name contains "Scheduled" AND team/name = "GCA Team"'
gt.pageSize = 1000
gt.orderBy = 'age /'
gt.orderBy = '_info/dateEntered'
gt = gt.get_tickets()
ls = list(gt)
try:
    for i in ls:
        ticketID = i.id
        contactNAME = i.contactName
        ticketSTATUS = i.status['name']
        ticketENTERDATE = i._info['dateEntered']
        # print(ticketID,contactNAME,ticketSTATUS,ticketENTERDATE)
        values = [ticketID, contactNAME, ticketSTATUS, ticketENTERDATE]
        zipped = zip(columns, values)
        a_dictionary = dict(zipped)
        # print(a_dictionary)
        data.append(a_dictionary)
except:
    pass

gt = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
gt.conditions = 'company/identifier="Georgia Cyber Academy" AND status/name contains "Re-Opened" AND team/name = "GCA Team"'
gt.pageSize = 1000
gt.orderBy = '_info/lastUpdated'
gt = gt.get_tickets()
ls = list(gt)
try:
    for i in ls:
        ticketID = i.id
        contactNAME = i.contactName
        ticketSTATUS = i.status['name']
        ticketENTERDATE = i._info['dateEntered']
        # print(ticketID,contactNAME,ticketSTATUS,ticketENTERDATE)
        values = [ticketID, contactNAME, ticketSTATUS, ticketENTERDATE]
        zipped = zip(columns, values)
        b_dictionary = dict(zipped)
        # print(b_dictionary)
        reOpenedList.append(b_dictionary)
except:
    pass


def getTicketNotes(TICKET_ID):
    ticket_notes = ticket_notes_api.TicketNotesAPI(url=cwURL, auth=cwAUTH, ticket_id=TICKET_ID)
    ticket_notes.pageSize = 1
    ticket_notes.orderBy = 'id desc'
    ticket_notes = ticket_notes.get_ticket_notes()
    ls = list(ticket_notes)
    x = 0
    for i in ls:
        # x += 1
        # print("\n","Note #",x,"\n[",i,"]")
        parentUPDATE = i._info['lastUpdated']
        parentCONTACT = i._info['updatedBy']
        dateCreated = i.dateCreated
    return (parentUPDATE, parentCONTACT, dateCreated)


def getTimeEntriesByTicketID(ticketID):
    gte = time_entries_api.TimeEntriesAPI(url=cwURL, auth=cwAUTH)
    gte.conditions = 'chargeToId={}'.format(ticketID)
    gte.pageSize = 1
    gte.orderBy = 'id desc'
    gte = gte.get_time_entries()
    ls = list(gte)
    for i in ls:
        engtimeEND = i.timeEnd
        engineer = i.member['identifier']
    return (engtimeEND, engineer)


def timeComparision(tID, contact, ferpaName):
    try:
        parentUPDATE, parentCONTACT, dateCreated = getTicketNotes(tID)
    except Exception as e:
        time.sleep(10)
        parentUPDATE, parentCONTACT, dateCreated = getTicketNotes(tID)
    # time.sleep(7)
    try:
        engtimeEND, engineer = getTimeEntriesByTicketID(tID)
    except Exception as e:
        time.sleep(10)
        engtimeEND, engineer = getTimeEntriesByTicketID(tID)
    # time.sleep(7)
    parentUPDATE = parser.parse(parentUPDATE)
    dateCreated = parser.parse(dateCreated)
    engtimeEND = parser.parse(engtimeEND)
    today = datetime.now(tz=pytz.UTC).replace(microsecond=0)
    # today = date.today()

    if parentUPDATE > engtimeEND:
        # print(tID,parentCONTACT,parentUPDATE)
        # print("parent contact: ",parentCONTACT)
        if parentCONTACT.endswith('@georgiacyber.org'):
            parentCONTACT = 'Staff'
        else:
            parentCONTACT = parentCONTACT.replace(contact, ferpaName)
            parentCONTACT = 'Parent/Student'
        difference = today - parentUPDATE
        if difference.days == 0:
            return ("0 days, " + str(difference)), parentCONTACT
        elif difference.days < 10:
            return ("0" + str(difference)), parentCONTACT
        else:
            return str(difference), parentCONTACT
    elif parentUPDATE < engtimeEND:
        # print(tID,engineer,'-',engtimeEND,x)
        difference = today - engtimeEND
        if difference.days == 0:
            return ("0 days, " + str(difference)), engineer
        elif difference.days < 10:
            return ("0" + str(difference)), engineer
        else:
            return str(difference), engineer


def json_to_sql(json_object):
    #### Test information
    statement = f"""
    DECLARE @jsonText NVARCHAR(MAX) ='{json_object}' 
    SELECT
        jt.[key] AS Ticket#
        ,ci.OrgID
        ,CONCAT(pc.Org_ID,' ',pc.FirstName,' ',LEFT(pc.LastName,1)) AS FERPA_Contact
        ,(CASE WHEN OD.AssetID IS NOT NULL THEN 'Outstanding Equipment' ELSE '' END) AS [Status]
        ,ISNULL(CAST(OD.AssetID AS NVARCHAR),'') AS AssetID
    FROM OPENJSON(@jsonText) as jt
    INNER JOIN GCAAssetMGMT_2_0.Pers.vwContactIndex AS ci
        ON jt.value = ci.CONTACT
    INNER JOIN GCAAssetMGMT_2_0.Pers.Contacts AS pc
        ON ci.PersonID = pc.PersonID
    LEFT JOIN (SELECT *,(CASE WHEN FamilyID > 0 THEN CONCAT('FID_',FamilyID) ELSE CONCAT('STF_',TRIM(Org_ID)) END) AS RefVal
                FROM GCAAssetMGMT_2_0.Asset.vwContactOutstandingDevices) AS od
        ON RefVal = (CASE 
                        WHEN pc.Student = 1 THEN CONCAT('FID_',pc.FamilyID)
                        WHEN pc.Staff = 1   THEN CONCAT('STF_',TRIM(pc.Org_ID)) END)
        """
    seventeen = pd.read_sql_query(statement, conn)
    df = pd.DataFrame(seventeen)
    return df


df2 = pd.DataFrame(data, columns=["Ticket #", "Contact", "Status", "Creation Date"])  # Create DataFrame
df3 = pd.DataFrame(reOpenedList, columns=["Ticket #", "Contact", "Status", "Creation Date"])
returns = df2.loc[df2['Status'].str.contains('Scheduled - Awaiting Hardware')]
address_change = df2.loc[~df2['Status'].str.contains('Awaiting Hardware')]


def returnChange():
    """
    Utilizes the SQL Proc UnreturnedDevCheck to check if the student has any unreturned equipment.
    """
    ct = pd.Series(returns['Contact'].values, index=returns['Ticket #']).to_dict()
    json_object = json.dumps(ct, indent=4)
    stringy_ct = str(json_object)
    json_object = stringy_ct.replace("'", "''")
    dflst = []
    outdf = pd.DataFrame()
    try:
        outstanding_equipment = json_to_sql(json_object)
        try:
            for i in range(len(outstanding_equipment)):
                # print(outstanding_equipment.loc[[i]])
                ticket = outstanding_equipment['Ticket#'].loc[i]
                contact = outstanding_equipment['OrgID'].loc[i]
                ferpaName = outstanding_equipment['FERPA_Contact'].loc[i]
                status = outstanding_equipment['Status'].loc[i]
                owed_asset = outstanding_equipment['AssetID'].loc[i]

                if status == 'Outstanding Equipment':
                    # print("Outstanding Equipment:\n",ticket,contact,ferpaName,status,owed_asset)
                    try:
                        tc, lc = timeComparision(ticket, contact, ferpaName)
                    except Exception as e:
                        print(e)
                        tc = "MANUAL VERIFICATION NEEDED"

                    data = {
                        'Ticket Number': ticket,
                        'Student': ferpaName,
                        'Asset Number': owed_asset,
                        'Return Status': status,
                        'Last Contact Time': tc,
                        'Last Contact': lc
                    }

                    # data= [ticket,ferpaName,'','Returned',timeComparision(ticket,contact,ferpaName)]
                    tempRetDF = pd.DataFrame(data, index=[i])
                    dflst.append(tempRetDF)
                    # print(dflst)
                    # time.sleep(3)

                else:
                    try:
                        tc, lc = timeComparision(ticket, contact, ferpaName)
                    except Exception as e:
                        print(e)
                        tc = "MANUAL VERIFICATION NEEDED"

                    data = {
                        'Ticket Number': ticket,
                        'Student': ferpaName,
                        'Asset Number': '',
                        'Return Status': 'Returned',
                        'Last Contact Time': tc,
                        'Last Contact': lc
                    }
                    try:
                        ticket_notes = ticket_notes_api.TicketNotesAPI(url=cwURL, auth=cwAUTH, ticket_id=ticket)
                        note = ticket_note.TicketNote(
                            {"text": "Equipment has been returned. Changed to New - Verification",
                             "detailDescriptionFlag": True, "internalAnalysisFlag": True, "externalFlag": False})
                        ticket_notes.create_ticket_note(note)
                        api = tickets_api.TicketsAPI(url=cwURL, auth=cwAUTH)
                        ticket = api.update_ticket(ticket, "/status/id", "584")
                    except Exception as e:
                        print('Couldn\'t update ticket type for Returned Device.')
                    # data= [ticket,ferpaName,'','Returned',timeComparision(ticket,contact,ferpaName)]
                    tempRetDF = pd.DataFrame(data, index=[i])
                    # tempRetDF = pd.DataFrame(data,columns = ['Ticket Number','Student','Asset Number','Return Status','Last Contact'])
                    dflst.append(tempRetDF)
                    # print(dflst)

            outdf = pd.concat(dflst)
            outdf = outdf.sort_values(['Return Status', 'Last Contact Time'], ascending=[False, False])
            # print(outdf)
        except Exception as e:
            print(e)

    except Exception as e:
        print(e)
    return (outdf)


def addressChange():
    dflst = []
    """
    Used for finding Address Changes based on SQL Proc UpdatedRecord and ASAPERLVerify.
    Currently only works for Students, and in conjuction with data = cp.getTickets().
    """
    # print(address_change)
    # contact = address_change['Contact'].iloc()
    # ticket = address_change['Ticket #'].iloc()

    # contact = address_change['Contact'].tolist()
    # ticket = address_change['Ticket #'].tolist()
    # print("\n")
    ct = pd.Series(address_change['Contact'].values, index=address_change['Ticket #']).to_dict()
    # print(ct)
    # print("len: ",len(ct))
    addressdf = pd.DataFrame()

    with indent(4, quote='>>>'):
        try:
            indexnumber = 0
            for ticket, contact in ct.items():
                x = re.sub("[^0-9]", "", contact)
                if x == "":
                    continue
                else:
                    NewAddress_query = f"EXEC GCAAssetMGMT_2_0.dbo.[uspUpdatedRecord] " + x
                    NewAddress = pd.read_sql(NewAddress_query, conn)

                    database_address_query = f"EXEC IsolatedSafety.dbo.[uspFamilyLookUp]  " + x
                    database_address = pd.read_sql(database_address_query, conn)
                    database_street = database_address.loc[database_address['StudentID'] == x]
                    ferpaName = database_address['FERPA_Contact'].loc[0]

                    database_street = database_address['LG_Street'].loc[0]
                    if not NewAddress.empty:
                        # print(database_address)
                        # database_street = database_address.iat[0,4]
                        try:
                            tc, lc = timeComparision(ticket, contact, ferpaName)
                        except Exception as e:
                            print(e)
                            tc = "MANUAL VERIFICATION NEEDED"
                        data = {
                            'Ticket Number': ticket,
                            'Student': ferpaName,
                            'Change Status': 'New Address',
                            'New Address': database_street,
                            'Last Contact Time': tc,
                            'Last Contact': lc
                        }
                        # data= [ticket,ferpaName,'','Returned',timeComparision(ticket,contact,ferpaName)]
                        indexnumber = +1
                        tempRetDF = pd.DataFrame(data, index=[indexnumber])
                        dflst.append(tempRetDF)

                    else:
                        try:
                            tc, lc = timeComparision(ticket, contact, ferpaName)
                        except Exception as e:
                            print(e)
                            tc = "MANUAL VERIFICATION NEEDED"
                        data = {
                            'Ticket Number': ticket,
                            'Student': ferpaName,
                            'Change Status': '',
                            'New Address': '',
                            'Last Contact Time': tc,
                            'Last Contact': lc
                        }

                        indexnumber = +1
                        tempRetDF = pd.DataFrame(data, index=[indexnumber])
                        dflst.append(tempRetDF)
            try:
                addressdf = pd.concat(dflst)
            except ValueError as e:
                print(e)
        except KeyError as keyerror:
            print("You are receiving a", keyerror)
    # if ct==0:
    #     puts(colored.cyan('No Address changes found.'))
    #     addresslog.info('No Address changes found.')
    # print("\n")
    return (addressdf)


time_now = localtime = time.asctime(time.localtime(time.time()))
sender_email = "mbrownscaadmin@georgiacyber.org"
coworkers = ["mbrown@sca-atl.com", "nbowman@sca-atl.com", "Aadeli@sca-atl.com", "emorris@sca-atl.com",
             "bdixon@sca-atl.com", "bkrusac@sca-atl.com"]
# coworkers= ["mbrown@sca-atl.com"]
password = (config['google']['mailapp'])
message = MIMEMultipart("alternative")
message["Subject"] = "Student Asset Returns and Address Changes " + str(time_now)
message["From"] = sender_email
message['To'] = ', '.join(coworkers)

html = """\
<html>
<head></head>
<body>
    <p style="font-size:30px"><b>Re-Opened Tickets</b></p>
    {}
    <br>
    <p style="font-size:30px"><b>Address Changes</b></p>
    {}
    <br>
    <p style="font-size:30px"><b>Returns</b></p>
    {}
</body>
</html>
""".format(build_table(df3, 'blue_light', font_size='14px', width='auth'),
           build_table(addressChange(), 'blue_light', font_size='14px', width='auth'),
           build_table(returnChange(), 'blue_light', font_size='14px', width='auth'))

part1 = MIMEText(html, 'html')
message.attach(part1)

context = ssl.create_default_context()
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(
        sender_email, coworkers, message.as_string())
    server.quit()


