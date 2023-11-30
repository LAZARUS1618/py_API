# NJUNS API CONNECTION
# ETHAN GUECK
# 10/10/23
# This script is intended to query the NJUNS API and output a JSON file. 
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)

query = '' # Insert Password
query_text = urllib.parse.quote(query)
api_url = r"https://client:secret@www.njuns.com/app/rest/v2/oauth/token?grant_type=password&username=########&password=***" # Insert User Name inplace of #######

api_url = api_url.replace("***", query_text)
print(api_url)
response = requests.post(api_url)
ack = response.json()
print(ack['access_token'])

def generate_query(ACCESS_TOKEN, API_URL, start_date, end_date, view):
    final_df = pd.DataFrame()

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }

    while start_date < end_date:
        date_from = start_date.strftime('%Y-%m-%d %H:%M:%S.000')
        date_to = (start_date + delta).strftime('%Y-%m-%d %H:%M:%S.000')

        payload = {
            "filter": {
                "conditions": [
                    {
                        "group": "and",
                        "conditions": [
                            {
                                "property": "createTs",
                                "operator": ">=",
                                "value": date_from
                            },
                            {
                                "property": "createTs",
                                "operator": "<=",
                                "value": date_to
                            }
                        ]
                    }
                ]
            },
            "view": view
        }

        response = requests.post(API_URL, headers=headers, json=payload)

        if response.status_code == 200:
            data = response.json()
            temp_df = pd.DataFrame(data)
            final_df = pd.concat([final_df, temp_df], ignore_index=True)
            print(f"Fetched data from {date_from} to {date_to}")
        else:
            print(f"Failed to fetch data for the period {date_from} to {date_to}")
        start_date += delta
    return final_df
def generate_query_tick(ACCESS_TOKEN, API_URL, start_date, end_date):
    final_df = pd.DataFrame()

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }

    while start_date < end_date:
        date_from = start_date.strftime('%Y-%m-%d %H:%M:%S.000')
        date_to = (start_date + delta).strftime('%Y-%m-%d %H:%M:%S.000')

        payload = {
            "filter": {
                "conditions": [{
                    "group": "and",
                    "conditions": [{
                        "property": "createTs",
                        "operator": ">=",
                        "value": "2022-01-01 00:00:00.000"
                    }]
                }]
            },
            "fields": ["ticketNumber", "createTs", "referenceId", "wfTitle", "numberOfAssets", "wfAssignedDate"]
        }
        response = requests.post(API_URL, headers=headers, json=payload)

        if response.status_code == 200:
            data = response.json()
            temp_df = pd.DataFrame(data)
            final_df = pd.concat([final_df, temp_df], ignore_index=True)
            print(f"Fetched data from {date_from} to {date_to}")
        else:
            print(f"Failed to fetch data for the period {date_from} to {date_to}")
        start_date += delta
    final_df.reset_index(drop=True, inplace=True)
    col_to_drop = ['_entityName', 'id', 'principalSet', 'longitude', 'contactName', 'latitude', 'houseNumber',
                   'assetId', 'option1', 'street1', 'contactEmail', 'createdBy', 'contactPhone', 'miscId',
                   'crossStreet']
    col_to_rename = {'_instanceName': 'TicketNumber_3', 'createTs': 'TicketCreatedOn', 'ticketId': 'TicketNumber_2',
                     'status': 'TicketStatus', 'ticketNumber': 'TicketNumber',
                     'numberOfAssets': 'TicketNumberOfAssets', 'updateTs': 'TicketUpdatedDate',
                     'remarks': 'TicketRemarks', 'referenceId': 'RefId', 'wfTitle': 'TicketWfTitle'}
    final_df = final_df.rename(columns=col_to_rename)
    final_df = final_df.drop(columns=col_to_drop)
    return final_df

API_URL1 = "https://njuns.com/app/rest/v2/entities/njuns$TicketWallEntry/search?view=ticketWallEntryView"
API_URL2 = "https://njuns.com/app/rest/v2/entities/njuns$Ticket/search?view=ticket-edit-view"
ACCESS_TOKEN = ack['access_token']
view1 = "ticketWallEntryBrowse-view"
view2 = "ticketWallEntry-browse-view"

start_date = datetime(2023, 10, 10)
end_date = datetime.now()
delta = timedelta(days=3)

df = generate_query(ACCESS_TOKEN, API_URL1, start_date, end_date, view1)
df2 = generate_query(ACCESS_TOKEN, API_URL1, start_date, end_date, view2)
df3 = generate_query_tick(ACCESS_TOKEN, API_URL2, start_date, end_date)
output_df = df.merge(df2, on="id", how='outer')
output_df2 = df3
print(output_df2.columns)

def extract_user_columns(row):
    user_data = row['user']
    return pd.Series(user_data)
def extract_ticket_columns(row):
    ticket_data = row['ticket']
    return pd.Series(ticket_data)
user_columns = output_df.apply(extract_user_columns, axis=1)
ticket_columns = output_df.apply(extract_ticket_columns, axis=1)
output_df = pd.concat([output_df, user_columns, ticket_columns], axis=1)
output_df = output_df.drop(['_entityName_x', '_instanceName_x', 'id', 'attachments_x', 'ticket',
                            'origin_x', 'type_x', 'version_x', 'flagged_x', 'principalSet_x',
                            '_entityName_y', '_instanceName_y', 'attachments_y',
                            'origin_y', 'type_y', 'version_y', 'flagged_y', 'principalSet_y',
                            'comment_y', 'user', '_entityName', '_instanceName', 'id',
                            'version', '_entityName', 'assetId',
                            'contactEmail', 'contactName', 'contactPhone', 'createTs', 'createdBy',
                            'houseNumber', 'id', 'latitude', 'longitude', 'miscId',
                            'numberOfAssets', 'principalSet', 'priority', 'referenceId', 'remarks',
                            'startDate', 'status', 'street1', 'ticketNumber',
                            'version'], axis=1)

print(output_df.head())
print(output_df.columns)
final_output_df = output_df.merge(output_df2, right_on='TicketNumber_2', left_on='ticketId', how='outer')
final_output_df.to_csv('output_json_modified4.csv', index=False)
#df3.to_csv('output_json_modified5.csv', index=False)
end_time = datetime.now()
