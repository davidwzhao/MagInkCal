from __future__ import print_function
import datetime as dt
import os.path
import pathlib
import logging

# from google.transit import gtfs_realtime_pb2
import gtfs_realtime_1007_extension_pb2 as gtfs_realtime_pb2
import requests
import zipfile
from io import BytesIO
import csv
import pytz

from datetime import datetime

def parse_time(t):
    ts = int(t)
    tz = pytz.timezone('Australia/Sydney')
    return datetime.fromtimestamp(ts, tz).strftime('%Y-%m-%d %H:%M:%S')

class SydTrainsHelper:
    def __init__(self):
        self.curr_path = str(pathlib.Path(__file__).parent.absolute())

        if os.path.exists(self.curr_path + '/apikey.txt'):
            with open(self.curr_path + '/apikey.txt', 'r') as token:
                self.apikey = token.read().strip()
        else:
            self.apikey = ''

    def download_timetable_info(self):
        # TODO: check the API header if we need to download a new version
        if os.path.exists(self.curr_path + '/timetable'):
            return

        headers = {'accept': 'application/octet-stream', 'Authorization': 'apikey ' + self.apikey}
        r = requests.get('https://api.transport.nsw.gov.au/v1/gtfs/schedule/sydneytrains', headers=headers)

        # Timetable info is returned as a .zip file containing:
        # - agency.txt
        # - calendar.txt
        # - routes.txt
        # - shapes.txt
        # - stops.txt
        # - stop_times.txt
        # - trips.txt
        # - vehicle_boardings.txt
        # - vehicle_categories.txt
        # - vehicle_couplings.txt

        # Unzip the file
        file_bytes = BytesIO(r.content)

        with zipfile.ZipFile(file_bytes) as zip_file:
            zip_file.extractall(path=self.curr_path + '/timetable')
            for name in zip_file.namelist():
                print("found a file!", name)

    def get_timetable_info(self):
        if os.path.exists(self.curr_path + '/timetable'):
            self.download_timetable_info()

        # Process stops
        stops_info = {}
        with open(self.curr_path + '/timetable/stops.txt', 'r') as stops_file:
            data = csv.DictReader(stops_file)

            for row in data:
                stop_id = row['stop_id']
                stop_name = row['stop_name']

                stops_info[stop_id] = stop_name

        self.stops_info = stops_info

        # print(stops_info)

    def get_realtime_data(self):
        headers = {'accept': 'application/x-google-protobuf', 'Authorization': 'apikey ' + self.apikey}
        r = requests.get('https://api.transport.nsw.gov.au/v2/gtfs/realtime/sydneytrains', headers=headers)

        return r.content

    def parse_response(self, response):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                # print(entity.trip_update)
                for update in entity.trip_update.stop_time_update:
                    print(self.stops_info[update.stop_id], parse_time(update.departure.time), update.departure.delay)

                # break
                print()





if __name__ == '__main__':
    syd_trains_helper = SydTrainsHelper()

    syd_trains_helper.get_timetable_info()

    response = syd_trains_helper.get_realtime_data()
    syd_trains_helper.parse_response(response)

    # print("hello:", syd_trains_helper.get_request())
