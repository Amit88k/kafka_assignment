from __future__ import print_function

import json
import pickle
import os.path
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from kafka import KafkaConsumer, KafkaProducer

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1IlC4uK0Sb3LTIrCEt30VzVaGovMs-vo8q3jaQdUkNrQ'
SAMPLE_RANGE_NAME = 'sheet1!A:E'


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9093'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def read_spread_sheet():
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

    topic_name = "topic2"
    # kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10,1))
    producer = connect_kafka_producer()
    if not values:
        print('No data found.')
    else:
        for row in values:
            print('%s, %s' % (row[0], row[1]))
            message = {"Id" : row[0], "Name" : row[1]}
            publish_message(producer, topic_name, "key", json.dumps(message))


if __name__ == '__main__':
    read_spread_sheet()
    # client id: 30484477302-ph1ef33p37l5lq3vcv65cvlt3m01nijc.apps.googleusercontent.com
    # client secret: FNuLauZepFeABy4c1Lncw7LS






