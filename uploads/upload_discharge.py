import json
import traceback
import sys
import os
from os.path import join as path_join
from datetime import datetime, timedelta
import re
import csv
import time

from db_adapter.logger import logger
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
# from db_adapter.constants import COMMON_DATE_TIME_FORMAT, CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, \
#     CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id, get_source_parameters
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_hechms_stations
from db_adapter.curw_fcst.timeseries import Timeseries

CURW_FCST_DATABASE = 'curw_fcst'
CURW_FCST_PASSWORD = 'cfcwm07'
CURW_FCST_USERNAME = 'curw'
CURW_FCST_PORT = '3306'
CURW_FCST_HOST = '192.168.1.43'
hechms_stations = {}


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][2:]

    return data


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        return None


def getUTCOffset(utcOffset, default=False):
    """
    Get timedelta instance of given UTC offset string.
    E.g. Given UTC offset string '+05:30' will return
    datetime.timedelta(hours=5, minutes=30))
    :param string utcOffset: UTC offset in format of [+/1][HH]:[MM]
    :param boolean default: If True then return 00:00 time offset on invalid format.
    Otherwise return False on invalid format.
    """
    offset_pattern = re.compile("[+-]\d\d:\d\d")
    match = offset_pattern.match(utcOffset)
    if match:
        utcOffset = match.group()
    else:
        if default:
            print("UTC_OFFSET :", utcOffset, " not in correct format. Using +00:00")
            return timedelta()
        else:
            return False

    if utcOffset[0] == "+":  # If timestamp in positive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0] == "-":  # If timestamp in negative zone, deduct it from current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=-1 * int(offset_str[0]), minutes=-1 * int(offset_str[1]))


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    print('LibForecastTimeseries:: extractForecastTimeseries')
    if by_day:
        extract_date_time = datetime.strptime(extract_date, '%Y-%m-%d')
    else:
        extract_date_time = datetime.strptime('%s %s' % (extract_date, extract_time), '%Y-%m-%d %H:%M:%S')

    is_date_time = isinstance(timeseries[0][0], datetime)
    new_timeseries = []
    for i, tt in enumerate(timeseries):
        tt_date_time = tt[0] if is_date_time else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
        if tt_date_time >= extract_date_time:
            new_timeseries = timeseries[i:]
            break

    return new_timeseries


def save_forecast_timeseries_to_db(pool, timeseries, run_date, run_time, tms_meta, fgt):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', tms_meta)

    # {
    #         'tms_id'     : '',
    #         'sim_tag'    : '',
    #         'station_id' : '',
    #         'source_id'  : '',
    #         'unit_id'    : '',
    #         'variable_id': ''
    #         }

    date_time = datetime.strptime('%s %s' % (run_date, run_time), COMMON_DATE_TIME_FORMAT)

    forecast_timeseries = []

    if 'utcOffset' in tms_meta:  # If there is an offset, shift by offset before proceed
        print('Shift by utcOffset:', tms_meta['utcOffset'].resolution)
        # Convert date time with offset
        date_time = date_time + tms_meta['utcOffset']
        for item in timeseries:
            forecast_timeseries.append(
                [datetime.strptime(item[0], COMMON_DATE_TIME_FORMAT) + tms_meta['utcOffset'], item[1]])
    else:
        forecast_timeseries = timeseries

    try:

        TS = Timeseries(pool=pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=tms_meta)

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=tms_meta)
            tms_meta['tms_id'] = tms_id
            TS.insert_run(run_meta=tms_meta)
            TS.update_start_date(id_=tms_id, start_date=fgt)

        TS.insert_data(timeseries=forecast_timeseries, tms_id=tms_id, fgt=fgt, upsert=True)
        TS.update_latest_fgt(id_=tms_id, fgt=fgt)

    except Exception:
        logger.error("Exception occurred while pushing data to the curw_fcst database")
        traceback.print_exc()


def extract_distrubuted_hechms_outputs(out_file_path, run_date, run_time):
    """
    Config.json
    {
      "output_file_name": "DailyDischarge.csv",
      "output_dir": "",
      "run_date": "2019-05-24",
      "run_time": "00:00:00",
      "utc_offset": "",
      "sim_tag": "hourly_run",
      "model": "HECHMS",
      "version": "single",
      "unit": "m3/s",
      "unit_type": "Instantaneous",
      "variable": "Discharge",
      "station_name": "Hanwella"
    }
    """
    try:

        config = json.loads(open('/home/curw/git/distributed_hechms/uploads/config.json').read())

        # output related details
        run_date = run_date
        run_time = run_time

        utc_offset = read_attribute_from_config_file('utc_offset', config, False)
        if utc_offset is None:
            utc_offset = ''

        # sim tag
        sim_tag = read_attribute_from_config_file('sim_tag', config, True)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = read_attribute_from_config_file('version', config, True)

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        # station details
        station_name = read_attribute_from_config_file('station_name', config, True)


        if not os.path.exists(out_file_path):
            msg = 'no file :: {}'.format(out_file_path)
            logger.warning(msg)
            print(msg)
            exit(1)

        fgt = get_file_last_modified_time(out_file_path)
        print("fgt, ", fgt)

        timeseries = read_csv(out_file_path)

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, db=CURW_FCST_DATABASE, user=CURW_FCST_USERNAME,
                        password=CURW_FCST_PASSWORD)

        hechms_stations = get_hechms_stations(pool=pool)

        station_id = hechms_stations.get(station_name)[0]
        lat = str(hechms_stations.get(station_name)[1])
        lon = str(hechms_stations.get(station_name)[2])

        source_id = get_source_id(pool=pool, model=model, version=version)

        variable_id = get_variable_id(pool=pool, variable=variable)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
            'sim_tag': sim_tag,
            'model': model,
            'version': version,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'latitude': lat,
            'longitude': lon,
            'station_id': station_id,
            'source_id': source_id,
            'variable_id': variable_id,
            'unit_id': unit_id
        }

        utcOffset = getUTCOffset(utc_offset, default=True)

        if utcOffset != timedelta():
            tms_meta['utcOffset'] = utcOffset

        # Push timeseries to database
        save_forecast_timeseries_to_db(pool=pool, timeseries=timeseries,
                                       run_date=run_date, run_time=run_time, tms_meta=tms_meta, fgt=fgt)
        return {'Result': 'Success'}
    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
        return {'Result': 'Fail'}