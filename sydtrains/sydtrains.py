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

from datetime import date, time, datetime, timedelta

def parse_time(t):
    # TODO: make this generic for timezone
    ts = int(t)
    tz = pytz.timezone('Australia/Sydney')
    return datetime.fromtimestamp(ts, tz) # .strftime('%Y-%m-%d %H:%M:%S')

def format_time(t):
    return t.strftime('%Y-%m-%d %H:%M:%S')

def scheduled():
    return gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Value('SCHEDULED')

class SydTrainsHelper:
    def __init__(self):
        self.curr_path = str(pathlib.Path(__file__).parent.absolute())

        if os.path.exists(self.curr_path + '/apikey.txt'):
            with open(self.curr_path + '/apikey.txt', 'r') as token:
                self.apikey = token.read().strip()
        else:
            self.apikey = ''

        # Set up config with relevant stops etc.
        self.setup_config()
        self.setup_timetable_info()
        self.setup_relevant_trips()

        # TODO: do all the other necessary setup things e.g., downloading timetable

    # TODO: just a mockup for now, do this properly and parse some json or something
    def setup_config(self):
        config = {}
        config["max_time_in_future"] = timedelta(hours=1)
        config["num_trips"] = 6
        config["home_stop"] = "Burwood"
        config["dest_stops"] = ["Newtown", "Wynyard"]
        config["timezone"] = pytz.timezone('Australia/Sydney')

        self.config = config

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

    def setup_timetable_info(self):
        if not os.path.exists(self.curr_path + '/timetable'):
            self.download_timetable_info()

        home_stop = self.config["home_stop"]
        dest_stops = self.config["dest_stops"]

        # Process stop_id to stop_name mapping. The result is a mapping
        #   stop_id -> stop_name
        stops_info = {}
        with open(self.curr_path + '/timetable/stops.txt', 'r') as stops_file:
            data = csv.DictReader(stops_file)

            for row in data:
                stop_id = row['stop_id']
                stop_name = row['stop_name']

                stops_info[stop_id] = stop_name

        self.stops_info = stops_info
        # print(stops_info)

        # Process timetable trip_id to stops and times mapping. The result should be a
        # mapping:
        #   trip_id -> [(stop_id_1, departure_time_1), (stop_id_2, departure_time_2), ...]
        timetable_info = {}

        today_date = date.today()
        print("today date", today_date)

        with open(self.curr_path + '/timetable/stop_times.txt', 'r') as timetable_file:
            data = csv.DictReader(timetable_file)

            # TODO: this data has too many rows, we need to speed it up or cache results (maybe in json???)
            for row in data:
                # Each row is a trip stop. E.g., it represents the trip `t_id` stopping at
                # stop `s_id`
                trip_id = row['trip_id']
                _departure_time = row['departure_time']

                h, m, s = tuple(map(int, _departure_time.split(':')))
                _today_date = today_date
                if h > 23:
                    _today_date += timedelta(days=1)
                    h -= 24

                departure_time = datetime.combine(_today_date, time(h, m, s))
                stop_id = row['stop_id']

                # Add the stop to timetable_info
                if trip_id not in timetable_info.keys():
                    # The stops are ordered, so it's important that it's a vector and not a
                    # set
                    timetable_info[trip_id] = []

                timetable_info[trip_id].append((stop_id, departure_time))

        self.timetable_info = timetable_info

        # for trip_id in timetable_info:
        #     print(trip_id)
        #
        #     for (stop_id, departure_time) in timetable_info[trip_id]:
        #         print('  ', stop_id, departure_time)

    def is_relevant_trip(self, trip):
        # Trip is a vector of the form [(stop_id, departure_time), ...]
        home_stop_idx = -1
        dest_stop_idxs = []

        for (idx, (stop_id, departure_time)) in enumerate(trip):
            # Make sure the stop is still scheduled, and not canceled or skipped
            # if update.schedule_relationship != gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Value('SCHEDULED'):
            #     continue

            if self.is_home_stop(stop_id):
                home_stop_idx = idx

            if self.is_dest_stop(stop_id):
                dest_stop_idxs.append(idx)

        # If the home stop doesn't exist, then this trip is not relevant
        if home_stop_idx == -1:
            return False

        # If there is a dest stop after the home stop, then this trip is relevant
        if any(map(lambda i : i > home_stop_idx, dest_stop_idxs)):
            return True

        return False

    def setup_relevant_trips(self):
        relevant_trips = {}

        for (trip_id, trip) in self.timetable_info.items():
            if self.is_relevant_trip(trip):
                relevant_trips[trip_id] = trip

        # print(relevant_trips)
        for trip_id in relevant_trips:
            print(trip_id)
        
            for (stop_id, departure_time) in relevant_trips[trip_id]:
                print('  ', stop_id, self.get_stop_name(stop_id), departure_time)

        self.relevant_trips = relevant_trips

    def get_stop_name(self, stop_id):
        return self.stops_info[stop_id]

    def is_home_stop(self, stop_id):
        stop_name = self.get_stop_name(stop_id)

        if stop_name.startswith(self.config["home_stop"]):
            return True

        return False

    def is_dest_stop(self, stop_id):
        stop_name = self.get_stop_name(stop_id)

        if any(map(lambda stop : stop_name.startswith(stop), self.config["dest_stops"])):
            return True

        return False

    def is_home_or_dest_stop(self, stop_id):
        return self.is_home_stop(stop_id) or self.is_dest_stop(stop_id)

    def get_realtime_data(self):
        headers = {'accept': 'application/x-google-protobuf', 'Authorization': 'apikey ' + self.apikey}
        r = requests.get('https://api.transport.nsw.gov.au/v2/gtfs/realtime/sydneytrains', headers=headers)

        return r.content

    def is_relevant_timetable_update_entity(self, entity):
        assert entity.HasField('trip_update')

        # if entity.trip_update.stop_time_update.schedule_relationship != 'SCHEDULED':
        #     return False

        # Compute the index in the trip of the home stop and dest stops
        home_stop_idx = -1
        dest_stop_idxs = []

        for (idx, update) in enumerate(entity.trip_update.stop_time_update):
            # Make sure the stop is still scheduled, and not canceled or skipped
            # if update.schedule_relationship != gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Value('SCHEDULED'):
            #     continue

            if self.is_home_stop(update.stop_id):
                home_stop_idx = idx

            if self.is_dest_stop(update.stop_id):
                dest_stop_idxs.append(idx)

        # If the home stop doesn't exist, then this trip is not relevant
        if home_stop_idx == -1:
            return False

        # If there is a dest stop after the home stop, then this trip is relevant
        if any(map(lambda i : i > home_stop_idx, dest_stop_idxs)):
            return True

        return False

        # has_home_stop = any(map(lambda update : self.is_home_stop(update.stop_id), entity.trip_update.stop_time_update))
        #
        # # The update also needs to have at least one dest stop
        # has_dest_stop = any(map(lambda update : self.is_dest_stop(update.stop_id), entity.trip_update.stop_time_update))
        #
        # return has_home_stop and has_dest_stop

    def get_timetable_updates(self, realtime_data):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(realtime_data)

        # Keep a set of relevant updates. This is in the format of a dict:
        #   trip_id -> (status, [(stop_id, departure_time, departure_delay), ...])
        updates = {}

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                # print(entity.trip_update)
                # has_relevant_stops = any(map(lambda update : self.is_home_or_dest_stop(update.stop_id), entity.trip_update.stop_time_update))

                # if self.is_relevant_timetable_update_entity(entity):
                trip_id = entity.trip_update.trip.trip_id
                status = entity.trip_update.trip.schedule_relationship

                stops = []

                # print(entity.trip_update)
                for update in entity.trip_update.stop_time_update:
                    stop_id = update.stop_id
                    departure_time = update.departure.time
                    departure_delay = update.departure.delay if update.departure.HasField('delay') else 0

                    stops.append((stop_id, departure_time, departure_delay))
                    print(trip_id, self.stops_info[update.stop_id], parse_time(update.departure.time), update.departure.delay)

                updates[trip_id] = (status, stops)

        # print(relevant_updates)
        return updates

    def _apply_update(self, trip, update):
        # trip is a vector of the form:
        #   [(stop_id, departure_time), ...]
        #
        # update is a vector of the form:
        #   [(stop_id, departure_time, departure_delay), ...]

        assert len(trip) == len(update)

        updated_trip = []

        for (stop, stop_update) in zip(trip, update):
            stop_id = stop[0]
            assert stop_id == stop_update[0]

            new_departure_time = stop_id[1] + timedelta(seconds=stop_update[2])

            updated_trip.append((stop_id, new_departure_time))

        return updated_trip


    # TODO: decide if this should take the updates set, or request it by itself
    def get_updated_relevant_trips(self, updates):
        # updates is a dict in the format of:
        #   trip_id -> (status, [(stop_id, departure_time, departure_delay), ...])

        # Get the relevant timetable, and apply any updates.
        # relevant_trips is a dict in the format of:
        #   trip_id -> [(stop_id, departure_time), ...]
        relevant_trips = self.relevant_trips

        updated_relevant_trips = {}

        # TODO: take into account the status of the trip
        for trip_id in relevant_trips:
            if trip_id in updates:
                updated_relevant_trips[trip_id] = self._apply_update(relevant_trips[trip_id], updates[trip_id])
                print("UPDATED TRIP!!", updated_relevant_trips[trip_id])
            else:
                updated_relevant_trips[trip_id] = relevant_trips[trip_id]

        return updated_relevant_trips




    # def get_alerts_data(self):
    #     headers = {'accept': 'application/x-google-protobuf', 'Authorization': 'apikey ' + self.apikey}
    #     r = requests.get('https://api.transport.nsw.gov.au/v2/gtfs/alerts/sydneytrains', headers=headers)
    #
    #     return r.content
    #
    # def get_relevant_alerts(self, alerts_data):
    #     feed = gtfs_realtime_pb2.FeedMessage()
    #     feed.ParseFromString(alerts_data)
    #
    #     for entity in feed.entity:
    #         print(entity)
    #
    # def get_positions_data(self):
    #     headers = {'accept': 'application/x-google-protobuf', 'Authorization': 'apikey ' + self.apikey}
    #     r = requests.get('https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains', headers=headers)
    #
    #     return r.content
    #
    # def get_relevant_positions(self, positions_data):
    #     feed = gtfs_realtime_pb2.FeedMessage()
    #     feed.ParseFromString(positions_data)
    #
    #     for entity in feed.entity:
    #         if entity.vehicle.trip.schedule_relationship != scheduled():
    #             print(entity)






if __name__ == '__main__':
    syd_trains_helper = SydTrainsHelper()

    # syd_trains_helper.setup_timetable_info()

    response = syd_trains_helper.get_realtime_data()
    timetable_updates = syd_trains_helper.get_timetable_updates(response)

    updated_timetable = syd_trains_helper.get_updated_relevant_trips(timetable_updates)

    # print("hello:", syd_trains_helper.get_request())

    # alerts = syd_trains_helper.get_alerts_data()
    # syd_trains_helper.get_relevant_alerts(alerts)
    #
    # positions = syd_trains_helper.get_positions_data()
    # syd_trains_helper.get_relevant_positions(positions)
