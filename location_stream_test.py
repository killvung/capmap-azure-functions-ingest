from google.protobuf.json_format import MessageToDict
from requests.exceptions import HTTPError
from pymongo import MongoClient

from datetime import datetime
import location_stream_pb2
import requests
import sched
import time

s = sched.scheduler(time.time, time.sleep)

url = 'https://data.texas.gov/download/eiei-9rpf/application%2Foctet-stream'
collection = MongoClient('localhost', 27017).capitalmetros.livelocations
read_location_stream = location_stream_pb2.FeedMessage()


def ingest():
    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.6
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.6
    else:
        read_location_stream.ParseFromString(response.content)
        entities = MessageToDict(read_location_stream)['entity']
        collection.insert_many(entities)
        s.enter(30, 1, ingest)

        now = datetime.now()
        print(now)


s.enter(30, 1, ingest)
s.run()
