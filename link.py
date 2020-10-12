from config import notion_URL, notion_token, pipedrive_key, pipedrive_domain
from notion.client import NotionClient
import requests
import datetime
from dateutil import parser as date_parser

client = NotionClient(token_v2=notion_token)
notion_table = client.get_collection_view(notion_URL)
pd_base_url = 'https://'+ pipedrive_domain +'.pipedrive.com/api/v1/'
def update_row(row, deal):
    row.title = deal['title']
    pipeline_data = requests.get(pd_base_url + "stages?pipeline_id=" + str(deal['pipeline_id'])
                                 + '&api_token='+ pipedrive_key).json()['data']
    for stage in pipeline_data:
        if(stage['id'] == deal['stage_id']):
            row.Pipeline = stage['pipeline_name']
            row.Stage = stage['name']
            break

    row.Next_Step = deal['next_activity_subject']
    last_updated = date_parser.parse(deal['update_time'])
    row.Idle_Time = str((datetime.date.today() - last_updated.date()).days) + " Days"
    row.ID = deal['id']
    row.URL = 'https://'+ pipedrive_domain +'.pipedrive.com/deal/' + str(deal['id'])


pd_api_response = requests.get(pd_base_url + 'deals?limi=500&api_token='+ pipedrive_key)
if pd_api_response.status_code != 200:
    print("Error: Pipedrive returned error ", pd_api_response.status_code)
    exit()
pd_deals = {}
for item in pd_api_response.json()['data']:
    id = item['id']
    pd_deals[id] = item

for row in notion_table.collection.get_rows():
    if(row.ID not in pd_deals.keys()):
        print(f'Removed row ID {row.ID}')
        row.remove()
        continue
    deal = pd_deals.pop(row.ID)
    print(f"Updating deal {deal['id']}")
    update_row(row, deal)

for id in pd_deals:
    deal = pd_deals[id]
    print(f"Created new deal for {id}")
    row = notion_table.collection.add_row()
    update_row(row, deal)
