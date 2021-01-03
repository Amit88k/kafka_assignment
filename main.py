from __future__ import print_function
import pickle
import os.path
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import kafka
# from kafka.consumer import setKafkaConsumer
from kafka import KafkaConsumer, KafkaProducer

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1IlC4uK0Sb3LTIrCEt30VzVaGovMs-vo8q3jaQdUkNrQ'
SAMPLE_RANGE_NAME = 'sheet1!A:E'

def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    topic = "topic2"
    # kafka_consumer = KafkaConsumer(topic,
    #                                auto_offset_reset='latest',
    #                                bootstrap_servers=[kafka_endpoint],
    #                                api_version=(0, 10),
    #                                consumer_timeout_ms=-1
    #                                )
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10,1))
    if not values:
        print('No data found.')
    else:
        for row in values:
            print('%s, %s' % (row[0], row[1]))
            kafka_producer.send(topic, row)


if __name__ == '__main__':
    main()
    # client id: 30484477302-ph1ef33p37l5lq3vcv65cvlt3m01nijc.apps.googleusercontent.com
    # client secret: FNuLauZepFeABy4c1Lncw7LS






