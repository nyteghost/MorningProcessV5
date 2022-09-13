import time
import requests
import json
import os
from _msGraph.auth import getAuth
from dotenv import load_dotenv
import pandas as pd
import imgkit
import numpy as np

import base64
import datetime
from pretty_html_table import build_table

load_dotenv()

groupId = os.getenv('groupId')
channelID = os.getenv('channelID')
code_collab = os.getenv('code_collab')
portal_posse = os.getenv('portal_posse')
mainDataChat = os.getenv('mainDataChat')

ENDPOINT = 'https://graph.microsoft.com/v1.0'
BETA = 'https://graph.microsoft.com/beta'


def get_pretty_json_string(value_dict):
    return json.dumps(value_dict, indent=4, sort_keys=True, ensure_ascii=False)


# def df2img(csv_file, name_file):
#     df = csv_file
#     df = df.style.set_table_styles([dict(selector='th', props=[('text-align', 'center'),('background-color', '#40466e'), ('color', 'white')])])
#     df.set_properties(**{'text-align': 'center'}).hide(axis='index')
#     pd.set_option('colheader_justify', 'center')
#     html = df.to_html()
#     options = {'quality': 80}
#     imgkit.from_string(html, name_file + ".png", options=options)


def df2html(dataframe):
    df = dataframe
    df = df.style.set_table_styles([dict(selector='th', props=[('text-align', 'center'),('background-color', '#40466e'), ('color', 'white')])])
    df.set_properties(**{'text-align': 'center'}).hide(axis='index')
    pd.set_option('colheader_justify', 'center')
    html = df.to_html()
    return html


class TeamsChat:
    def __init__(self, chat_ID):
        if chat_ID == "portal_posse":
            self.chat_ID = os.getenv('portal_posse')
        elif chat_ID == "mainDataChat":
            self.chat_ID = os.getenv('mainDataChat')
        elif chat_ID == "code_collab":
            self.chat_ID = os.getenv('code_collab')
        elif chat_ID == "ccmChat":
            self.chat_ID = os.getenv('ccmChat')
        elif chat_ID == "mpReport":
            self.chat_ID = os.getenv('mpReport')
        self.funcResult = getAuth('teams')
        self.headers = {
                    'Accept': 'application/json',
                   'Authorization': 'Bearer '+self.funcResult['access_token'],
                   'Content-Type': 'application/json'
                    }

    def send(self, message, mention=None):
        chatRoom = f"/chats/{self.chat_ID}/messages"
        url = BETA+chatRoom
        if mention is None:
            payload = json.dumps({
                "body": {
                    "contentType": "html",
                    "content": message
                }
            })
        elif mention:
            getPeople = self.getPeople()
            for i in getPeople['value']:
                if mention in i['displayName']:
                    mention = i['displayName']
                    userID = i['userId']
                    mentioned = f'<at id=\"0\">{mention}</at><br>'
                    payload = json.dumps({
                        "body": {
                            "contentType": "html",
                            "content": mentioned+message
                        },
                        "mentions": [
                            {
                                "id": 0,
                                "mentionText": mention,
                                "mentioned": {
                                    "user": {
                                        "displayName": mention,
                                        "id": userID,
                                        "userIdentityType": "aadUser"
                                    }
                                }
                            }
                        ]
                    })
                else:
                    pass
        res = requests.post(url, headers=self.headers, data=payload)
        print(res.text)

    # def sendImage(self, dataframe):
    #     df = dataframe
    #     df2img(df, 'dataframe')
    #     img = "dataframe.png"
    #     with open(img, "rb") as img_file:
    #         b64_string = base64.b64encode(img_file.read())
    #     xyz = b64_string.decode('utf-8')
    #
    #     chatRoom = f"/chats/{self.chat_ID}/messages"
    #     url = BETA+chatRoom
    #     payload = json.dumps({
    #         "body": {
    #             "contentType": "html",
    #             "content": "<div><div>\n<div><span><img height=\"297\" src=\"../hostedContents/1/$value\" width=\"297\" style=\"vertical-align:bottom; width:297px; height:297px\"></span>\n\n</div>\n\n\n</div>\n</div>"
    #         },
    #         "hostedContents": [
    #             {
    #                 "@microsoft.graph.temporaryId": "1",
    #                 "contentBytes": f"{xyz}",
    #                 "contentType": "image/png"
    #             }
    #         ]
    #     })
    #     res = requests.post(url, headers=self.headers, data=payload)
    #     print(res.text)

    def sendTable(self, title, font_size, dataframe):
        font = f'<font size="{font_size}"><center>{title}</center></font>'
        table = build_table(dataframe, 'green_dark', font_size='14px', width='auth', odd_bg_color="black")
        chatRoom = f"/chats/{self.chat_ID}/messages"
        url = BETA+chatRoom
        payload = json.dumps({
            "body": {
                "contentType": "html",
                "content": font+table
            }
        })
        res = requests.post(url, headers=self.headers, data=payload)
        print(res.text)

    def receive(self, top):
        funcResult = getAuth('teams')
        url = ENDPOINT + f'/chats/{self.chat_ID}/messages?$top={top}'
        result = requests.get(url, headers=self.headers).json()
        # result = json.loads(result.text)
        sxJSON = json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False)
        print(sxJSON)

    def findChats(self):
        url = ENDPOINT + '/me/chats'
        result = requests.get(url, headers=self.headers).json()
        # result = json.loads(result.text)
        sxJSON = json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False)
        for data in result['value']:
            print(data['topic'])
            print(data['id'])
            print()

    def getPeople(self):
        url = ENDPOINT + f'/chats/{self.chat_ID}/members'
        result = requests.get(url, headers=self.headers).json()
        # result = json.loads(result.text)
        sxJSON = json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False)
        print(sxJSON)
        return result

if __name__ == '__main__':
    print('Running as Main')
    df = pd.DataFrame(np.random.randint(0, 100, size=(15, 4)), columns=list('ABCD'))
    teamchat = TeamsChat('portal_posse')
    teamchat.send('Two more to test from warehouse.')