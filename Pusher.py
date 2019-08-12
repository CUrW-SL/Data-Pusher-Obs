#!/usr/bin/python3

import argparse
import json
import os
import pytz
from datetime import datetime
from datetime import timedelta

from curwmysqladapter import MySQLAdapter, Station
from db_adapter.constants import CURW_OBS_HOST, CURW_OBS_PORT, CURW_OBS_USERNAME, CURW_OBS_PASSWORD, CURW_OBS_DATABASE
from db_adapter.base import get_Pool, destroy_Pool
from Utils import \
    generate_curw_obs_hash_id, \
    extract_n_push_precipitation, \
    extract_n_push_temperature, \
    extract_n_push_windspeed, \
    extract_n_push_windgust, \
    extract_n_push_humidity, \
    extract_n_push_solarradiation, \
    extract_n_push_winddirection, \
    extract_n_push_waterlevel

USERNAME = "curw"
PASSWORD = "curw@123"
HOST = "10.138.0.6"
PORT = 3306
DATABASE = "curw_obs"

def utc_to_sl(utc_dt):
    sl_timezone = pytz.timezone('Asia/Colombo')
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(tz=sl_timezone)

try:
    ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
    COMMON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    forceInsert = False

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Configuration file that includes db configs and stations. Default is ./CONFIG.json.')
    parser.add_argument('-f', '--force', action='store_true', help='Enables force insert.')
    args = parser.parse_args()

    print('\n\nCommandline Options:', args)

    if args.config:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, args.config)).read())
    else:
        CONFIG = json.loads(open(os.path.join(ROOT_DIR, './CONFIG.json')).read())
    forceInsert = args.force

    weather_stations = CONFIG['weather_stations']
    water_level_stations = CONFIG['water_level_stations']
    stations = weather_stations + water_level_stations


    extract_from_db = CONFIG['extract_from']
    push_to_db = CONFIG['push_to']

    extract_adapter = MySQLAdapter(
        host=extract_from_db['MYSQL_HOST'],
        user=extract_from_db['MYSQL_USER'],
        password=extract_from_db['MYSQL_PASSWORD'],
        db=extract_from_db['MYSQL_DB'])

    # Prepare start and date times.
    now_date = utc_to_sl(datetime.now())
    # now_date = datetime.now()
    start_datetime_obj = now_date - timedelta(hours=2)
    end_datetime_obj = now_date
    start_datetime = start_datetime_obj.strftime(COMMON_DATE_FORMAT)
    end_datetime = end_datetime_obj.strftime(COMMON_DATE_FORMAT)

    # start_datetime = '2018-07-04 00:00:00'
    # end_datetime = '2018-07-31 00:00:00'

    for station in stations:
        print("**************** Station: %s, start_date: %s, end_date: %s **************"
              % (station['name'], start_datetime, end_datetime))

        variables = station['variables']
        if not isinstance(variables, list) or not len(variables) > 0:
            print("Station's variable list is not valid.", variables)
            continue

        station_name = station['name']
        latitude = station['station_meta'][2]
        longitude = station['station_meta'][3]
        units = station['units']
        unit_types = station['unit_type']
        description = station['description']

        for variable, unit, unit_type in zip(variables, units, unit_types):
            #print("**********")
            #(variable)
            #print(unit_type)
            #print(unit)
            #print("**********")
            #pool = get_Pool(host=CURW_OBS_HOST, port=CURW_OBS_PORT, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD, db=CURW_OBS_DATABASE)
            pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)
            obs_hash_id = generate_curw_obs_hash_id(pool, variable=variable, unit=unit, unit_type=unit_type,
                                                    latitude=latitude, longitude=longitude, station_name=station_name, description=description,
                                                    start_date=start_datetime)
            if variable == 'Precipitation':
                try:
                    extract_n_push_precipitation(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing precipitation.", ex)
            elif variable == 'Temperature':
                try:
                    extract_n_push_temperature(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing temperature.", ex)
            elif variable == 'WindSpeed':
                try:
                    extract_n_push_windspeed(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing wind-speed.", ex)
            elif variable == 'WindGust':
                try:
                    extract_n_push_windgust(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing wind-gust.", ex)
            elif variable == 'Humidity':
                try:
                    extract_n_push_humidity(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing humidity", ex)
            elif variable == 'SolarRadiation':
                try:
                    extract_n_push_solarradiation(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing solar-radiation", ex)
            elif variable == 'WindDirection':
                try:
                    extract_n_push_winddirection(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing solar-radiation", ex)
            elif variable == 'Waterlevel':
                try:
                    extract_n_push_waterlevel(extract_adapter, station, start_datetime, end_datetime, pool, obs_hash_id)
                except Exception as ex:
                    print("Error occured while pushing water-level", ex)
            else:
                print("Unknown variable type: %s" %variable)

except Exception as ex:
    print('Error occurred while extracting and pushing data:', ex)

