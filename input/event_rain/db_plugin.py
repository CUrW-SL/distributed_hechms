from decimal import Decimal
from shapely.geometry import Polygon, Point
import geopandas as gpd
import pandas as pd
from datetime import datetime, timedelta

MISSING_VALUE = -99999
FILL_VALUE = 0


def get_single_result(db_connection, query):
    cur = db_connection.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row


def get_multiple_result(db_connection, query):
    cur = db_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows


def get_basin_available_stations_timeseries(obs_connection, fcst_connection, sim_connection, shape_file,
                                            date_limits, sim_tag, wrf_model, max_error):
    stations_info = {}
    basin_available_stations = get_available_stations_ids(sim_connection, shape_file, date_limits[0])
    print('get_basin_available_stations_timese, ries|basin_available_stations: ', basin_available_stations)
    cell_map = get_cell_mapping(sim_connection, basin_available_stations)
    print('get_basin_available_stations_timeseries|cell_map : ', cell_map)
    for [obs_station_id, fcst_station_id] in cell_map:
        station_df = get_station_tms(obs_connection, fcst_connection, obs_station_id, fcst_station_id, date_limits,
                                     sim_tag, wrf_model, max_error)
        print('get_basin_available_stations_timeseries|station_df : ', station_df)
        if station_df is not None:
            row = get_station_info(obs_connection, obs_station_id)
            station_name = row['name']
            latitude = row['latitude']
            longitude = row['longitude']
            stations_info[station_name] = {'latitude': latitude, 'longitude': longitude, 'timeseries': station_df}
    return stations_info


def get_station_tms(obs_connection, fcst_connection, obs_station_id, fcst_station_id, date_limits, sim_tag, wrf_model, max_error):
    sql_query = 'select id from curw_obs.run where station={} and variable=10 and unit=9 ' \
                'and end_date>\'{}\''.format(obs_station_id, date_limits[0])
    row = get_single_result(obs_connection, sql_query)
    print('get_station_tms|row : ', row)
    obs_hash_id = get_single_result(obs_connection, sql_query)['id']
    print('get_obs_station_tms|obs_hash_id : ', obs_hash_id)
    if obs_hash_id is not None:
        obs_df = get_obs_timeseries_by_id(obs_connection, obs_hash_id, date_limits[0], date_limits[1], max_error)
        if obs_df is not None:
            fcst_df = get_fcst_timeseries_by_id(fcst_connection, fcst_station_id, sim_tag, wrf_model, date_limits[1],
                                                date_limits[2])
            station_df = obs_df.append(fcst_df)
            return station_df
    return None


def validate_dataframe(df, allowed_error):
    row_count = len(df.index)
    missing_count = df['value'][df['value'] == MISSING_VALUE].count()
    df_error = missing_count / row_count
    print('validate_dataframe|[row_count, missing_count, df_error]:',
          [row_count, missing_count, df_error, allowed_error])
    if df_error > allowed_error:
        print('Invalid')
        return False
    else:
        print('Valid')
        return True


def get_available_stations_ids(sim_connection, shape_file, date_time):
    """
    To get station information where it has obs_end for before the given limit
    :param date_time: '2019-08-27 05:00:00'
    :param model:
    :param method:
    :return: {station_name:{'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude},
    station_name1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1}}
    """
    available_stations = []
    print('get_available_stations_ids|date_time : ', date_time)
    try:
        sql = 'select id, grid_id, latitude, longitude from curw_sim.run where model=\'hechms\' ' \
                  'and method=\'MME\' and obs_end>=\'{}\''.format( date_time)
        print('sql : ', sql)
        results = get_multiple_result(sim_connection, sql)
        for row in results:
            station_id = row['grid_id'].split('_')[1]
            latitude = Decimal(row['latitude'])
            longitude = Decimal(row['longitude'])
            if is_inside_basin(shape_file, latitude, longitude):
                available_stations.append(station_id)
    except Exception as e:
        print('get_available_stations_ids|Exception:', e)
    finally:
        return available_stations


def is_inside_basin(shape_file, latitude, longitude):
    shape_attribute = ['OBJECTID', 1]
    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]
    if Point(longitude, latitude).within(shape_polygon):
        return True
    else:
        return False


def get_cell_mapping(sim_connection, obs_station_list):
    obs_stations = set(obs_station_list)
    cell_map = []
    query = 'select grid_id, d03_1 from curw_sim.grid_map_obs'
    rows = get_multiple_result(sim_connection, query)
    print('get_cell_mapping|rows : ', rows)
    for row in rows:
        grid_id = row['grid_id']
        observed_id = grid_id.split('_')[1]
        if observed_id in obs_stations:
            forecast_id = row['d03_1']
            cell_map.append([observed_id, forecast_id])
    return cell_map


def get_obs_timeseries_by_id(obs_connection, hash_id, timeseries_start, timeseries_end, max_error):
    data_sql = 'select time,value from curw_obs.data where id=\'{}\' and time >= \'{}\' ' \
                 'and time <= \'{}\''.format(hash_id, timeseries_start, timeseries_end)
    try:
        print('data_sql : ', data_sql)
        results = get_multiple_result(obs_connection, data_sql)
        print('get_obs_timeseries_by_id|results : ', results)
        if len(results) > 0:
            time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                 - datetime.strptime(timeseries_start,'%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * 5))
            print('timeseries_start : {}'.format(timeseries_start))
            print('timeseries_end : {}'.format(timeseries_end))
            print('time_step_count : {}'.format(time_step_count))
            print('len(results) : {}'.format(len(results)))
            data_error = ((time_step_count - len(results)) / time_step_count)
            if data_error < 0:
                df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                return df
            elif data_error < max_error:
                print('data_error : {}'.format(data_error))
                print('filling missing data.')
                formatted_ts = []
                i = 0
                for step in range(time_step_count + 1):
                    tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * 5)
                    if step < len(results):
                        if tms_step == results[i]['time']:
                            formatted_ts.append(results[i])
                        else:
                            formatted_ts.append({'time': tms_step, 'value': Decimal(0)})
                    else:
                        formatted_ts.append({'time': tms_step, 'value': Decimal(0)})
                    i += 1
                df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
                print('get_station_timeseries|df: ', df)
                return df
            else:
                print('data_error : {}'.format(data_error))
                print('Data error is too large')
                return None
        else:
            print('No data.')
            return None
    except Exception as e:
        print('get_timeseries_by_id|data fetch|Exception:', e)
        return None


def get_fcst_timeseries_by_id(fcst_connection, station_id, sim_tag, wrf_model, timeseries_start, timeseries_end):
    hash_sql = 'select id from curw_fcst.run where sim_tag=\'{}\' and station=\'{}\' and source={} ' \
               'and variable=1 and unit=1 ;'.format(sim_tag, station_id, wrf_model)
    print('get_fcst_timeseries_by_id|hash_sql : ', hash_sql)
    row = get_single_result(fcst_connection, hash_sql)
    print('get_fcst_timeseries_by_id|row : ', row)
    hash_id = row['id']
    if hash_id is not None:
        data_sql = 'select time,value from curw_fcst.data where id=\'{}\' and time>\'{}\' ' \
                   'and time <= \'{}\';'.format(hash_id, timeseries_start, timeseries_end)
        rows = get_multiple_result(fcst_connection, data_sql)
        df = pd.DataFrame(data=rows, columns=['time', 'value'])
        df['time'] = pd.to_datetime(df['time'])
        df = (df.set_index('time').resample('5T').first().reset_index().reindex(columns=df.columns))
        df.set_index(keys='time')
        df.interpolate(method='linear', limit_direction='forward')
        return df
    return None


def get_station_info(obs_connection, station_id):
    sql_query = 'select name,latitude,longitude from curw_obs.station where ' \
                'station_type=\'CUrW_WeatherStation\' and id={};'.format(station_id)
    row = get_single_result(obs_connection, sql_query)
    return row
    #{station_name: {'latitude': latitude, 'longitude': longitude}}