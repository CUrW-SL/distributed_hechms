import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
from shapely.geometry import Polygon, Point
import geopandas as gpd
import os
from config import RESOURCE_PATH

MISSING_VALUE = -99999
FILL_VALUE = 0


def validate_dataframe(df, allowed_error):
    row_count = len(df.index)
    missing_count = df['value'][df['value'] == MISSING_VALUE].count()
    print('validate_dataframe|row_count : ', row_count)
    print('validate_dataframe|missing_count : ', missing_count)
    df_error = missing_count/row_count
    print('validate_dataframe|df_error : ', df_error)
    print('validate_dataframe|[row_count, missing_count, df_error]:', [row_count, missing_count, df_error, allowed_error])
    if df_error > allowed_error:
        print('Invalid')
        return False
    else:
        print('Valid')
        return True


def get_null_count(results):
    df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
    missing_count = df['value'][df['value'] == MISSING_VALUE].count()
    return missing_count


def is_in_basin(station_info, basin='kub'): #{'station': station, 'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}
    # print('is_in_basin|station_info : ', station_info)
    if basin == 'kub':
        shape_file = os.path.join(RESOURCE_PATH, 'kub-wgs84/kub-wgs84.shp')
    else:
        shape_file = os.path.join(RESOURCE_PATH, 'klb-wgs84/klb-wgs84.shp')
    shape_attribute = ['OBJECTID', 1]
    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]
    if Point(station_info['longitude'], station_info['latitude']).within(shape_polygon):  # make a point and see if it's in the polygon
        # print('is_in_basin|True|station_info : ', station_info)
        return True
    else:
        return False


class CurwSimAdapter:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db):
        print('[mysql_user, mysql_password, mysql_host, mysql_db] : ',
              [mysql_user, mysql_password, mysql_host, mysql_db])
        try:
            self.connection = mysql.connector.connect(user=mysql_user,
                                                      password=mysql_password,
                                                      host=mysql_host,
                                                      database=mysql_db)
            self.cursor = self.connection.cursor(buffered=True)
        except ConnectionError as ex:
            print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_flo2d_tms_ids(self, model, method):
        id_date_list = []
        cursor = self.cursor
        try:
            sql = 'select id,obs_end from curw_sim.run where model=\'{}\' and method=\'{}\' '.format(model, method)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                id_date_list.append([row[0], row[1]])
        except Exception as e:
            print('save_init_state|Exception:', e)
        finally:
            return id_date_list

    def get_flo2d_tms_ids(self, model, method):
        id_date_list = []
        cursor = self.cursor
        try:
            sql = 'select id,grid_id,obs_end from curw_sim.run where model=\'{}\' and method=\'{}\' '.format(model,
                                                                                                             method)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                id_date_list.append({'hash_id': row[0], 'grid_id': row[1], 'obs_end': row[2]})
        except Exception as e:
            print('save_init_state|Exception:', e)
        finally:
            return id_date_list

    def get_cell_timeseries(self, timeseries_start, timeseries_end, hash_id, res_mins):
        cursor = self.cursor
        try:
            sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<\'{}\' and id=\'{}\' '.format(
                timeseries_start, timeseries_end, hash_id)
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                # return pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                return pd.DataFrame(data=results, columns=['time', 'value'])
            else:
                return None
        except Exception as e:
            print('get_cell_timeseries|Exception:', e)
            return None

    def get_station_timeseries(self, timeseries_start, timeseries_end, station_name, source, model='hechms',
                               value_interpolation='MME', grid_interpolation='MDPA', acceppted_error=40):
        cursor = self.cursor
        try:
            grid_id = 'rainfall_{}_{}'.format(station_name, grid_interpolation)
            sql = 'select id, obs_end from curw_sim.run where model=\'{}\' and method=\'{}\'  and grid_id=\'{}\''.format(
                model, value_interpolation, grid_id)
            print('sql : ', sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                hash_id = result[0]
                print('hash_id : ', hash_id)
                data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
                    timeseries_start, timeseries_end, hash_id)
                try:
                    print('data_sql : ', data_sql)
                    cursor.execute(data_sql)
                    results = cursor.fetchall()
                    # print('results : ', results)
                    if len(results) > 0:
                        time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                               - datetime.strptime(timeseries_start,
                                                                   '%Y-%m-%d %H:%M:%S')).total_seconds() / (60 * 5))
                        print('timeseries_start : {}'.format(timeseries_start))
                        print('timeseries_end : {}'.format(timeseries_end))
                        print('time_step_count : {}'.format(time_step_count))
                        print('len(results) : {}'.format(len(results)))
                        data_error = ((time_step_count - len(results)) / time_step_count) * 100
                        if data_error < 1:
                            df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                            return df
                        elif data_error <= acceppted_error:
                            print('data_error : {}'.format(data_error))
                            print('filling missing data.')
                            formatted_ts = []
                            i = 0
                            for step in range(time_step_count):
                                tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                                    minutes=step * 5)
                                if step < len(results):
                                    if tms_step == results[i][0]:
                                        formatted_ts.append(results[i])
                                    else:
                                        formatted_ts.append((tms_step, Decimal(0)))
                                else:
                                    formatted_ts.append((tms_step, Decimal(0)))
                                i += 1
                            df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
                            print('get_station_timeseries|df: ', df)
                            return df
                        else:
                            print('Missing data.')
                            return None
                    else:
                        print('No data.')
                        return None
                except Exception as e:
                    print('get_station_timeseries|data fetch|Exception:', e)
                    return None
            else:
                print('No hash id.')
                return None
        except Exception as e:
            print('get_station_timeseries|Exception:', e)
            return None

    # def get_timeseries_by_id(self, hash_id, timeseries_start, timeseries_end, time_step_size=5):
    #     cursor = self.cursor
    #     data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
    #         timeseries_start, timeseries_end, hash_id)
    #     try:
    #         print('data_sql : ', data_sql)
    #         cursor.execute(data_sql)
    #         results = cursor.fetchall()
    #         # print('results : ', results)
    #         if len(results) > 0:
    #             time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
    #                                    - datetime.strptime(timeseries_start,
    #                                                        '%Y-%m-%d %H:%M:%S')).total_seconds() / (
    #                                           60 * time_step_size))
    #             print('timeseries_start : {}'.format(timeseries_start))
    #             print('timeseries_end : {}'.format(timeseries_end))
    #             print('time_step_count : {}'.format(time_step_count))
    #             print('len(results) : {}'.format(len(results)))
    #             data_error = ((time_step_count - len(results)) / time_step_count) * 100
    #             print('data_error : {}'.format(data_error))
    #             if data_error < 0:
    #                 df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
    #                 return df
    #             elif data_error < 5:
    #                 print('data_error : {}'.format(data_error))
    #                 print('filling missing data.')
    #                 formatted_ts = []
    #                 i = 0
    #                 for step in range(time_step_count + 1):
    #                     tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
    #                         minutes=step * time_step_size)
    #                     # print('tms_step : ', tms_step)
    #                     if step < len(results):
    #                         if tms_step == results[i][0]:
    #                             formatted_ts.append(results[i])
    #                         else:
    #                             formatted_ts.append((tms_step, Decimal(0)))
    #                     else:
    #                         formatted_ts.append((tms_step, Decimal(0)))
    #                     i += 1
    #                 df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
    #                 print('get_station_timeseries|df: ', df)
    #                 return df
    #             else:
    #                 print('data_error : {}'.format(data_error))
    #                 print('Data error is too large')
    #                 return None
    #         else:
    #             print('No data.')
    #             return None
    #     except Exception as e:
    #         print('get_timeseries_by_id|data fetch|Exception:', e)
    #         return None

    def get_timeseries_by_id(self, hash_id, timeseries_start, timeseries_end, allowed_error, time_step_size=5):
        cursor = self.cursor
        data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<=\'{}\' and id=\'{}\' '.format(
            timeseries_start, timeseries_end, hash_id)
        try:
            print('data_sql : ', data_sql)
            cursor.execute(data_sql)
            results = cursor.fetchall()
            if len(results) > 0:
                time_step_count = int((datetime.strptime(timeseries_end, '%Y-%m-%d %H:%M:%S')
                                       - datetime.strptime(timeseries_start,
                                                           '%Y-%m-%d %H:%M:%S')).total_seconds() / (
                                              60 * time_step_size))
                print('get_timeseries_by_id|timeseries_start : {}'.format(timeseries_start))
                print('get_timeseries_by_id|timeseries_end : {}'.format(timeseries_end))
                print('get_timeseries_by_id|time_step_count : {}'.format(time_step_count))
                print('get_timeseries_by_id|len(results) : {}'.format(len(results)))
                missing_count = time_step_count - len(results)
                print('get_timeseries_by_id|missing_count : {}'.format(missing_count))
                missing_count = get_missing_count(results) + missing_count
                print('get_timeseries_by_id|missing_count : {}'.format(missing_count))
                data_error = (missing_count / time_step_count)
                print('get_timeseries_by_id|data_error : {}'.format(data_error))
                if data_error < 0:
                    df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                    return df
                elif data_error <= allowed_error:
                    print('get_timeseries_by_id|data_error : {}'.format(data_error))
                    print('get_timeseries_by_id|filling missing data.')
                    formatted_ts = []
                    i = 0
                    for step in range(time_step_count + 1):
                        tms_step = datetime.strptime(timeseries_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * time_step_size)
                        # print('tms_step : ', tms_step)
                        if step < len(results):
                            if tms_step == results[i][0]:
                                formatted_ts.append(results[i])
                            else:
                                formatted_ts.append((tms_step, Decimal(0)))
                        else:
                            formatted_ts.append((tms_step, Decimal(0)))
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

    def get_available_stations(self, date_time, model='hechms', method='MME'):
        available_list = []
        print('get_available_stations|date_time : ', date_time)
        cursor = self.cursor
        try:
            sql = 'select id,grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\'  and obs_end>=\'{}\''.format(
                model, method, date_time)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                hash_id = row[0]
                station = row[1].split('_')[2]
                available_list.append([hash_id, station])
        except Exception as e:
            print('get_available_stations|Exception:', e)
        finally:
            return available_list

    def get_available_stations_info(self, date_time, exec_datetime, model='hechms', method='MME'):
        """
        To get station information where it has obs_end for before the given limit
        :param date_time: '2019-08-27 05:00:00'
        :param model:
        :param method:
        :return: {station_name:{'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude},
        station_name1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1}}
        """
        available_stations = {}
        print('get_available_stations_info| [date_time, exec_datetime] : ', [date_time, exec_datetime])
        last_hour = datetime.strptime(exec_datetime, '%Y-%m-%d %H:%M:%S') - timedelta(hours=1)
        last_hour = last_hour.strftime('%Y-%m-%d %H:%M:%S')
        print('get_available_stations_info| last_hour : ', last_hour)
        cursor = self.cursor
        try:
            sql = 'select id, grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\'  and obs_end>=\'{}\''.format(
                model, method, last_hour)
            # sql = 'select id, grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\' '.format(model, method)
            print('sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                hash_id = row[0]
                station = row[1].split('_')[1]
                latitude = Decimal(row[2])
                longitude = Decimal(row[3])
                available_stations[station] = {'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}
        except Exception as e:
            print('get_available_stations_info|Exception:', e)
        finally:
            return available_stations

    def get_available_stations_in_sub_basin(self, sub_basin_shape_file, date_time, model, method, exec_datetime):
        """
        Getting station points resides in the given shapefile
        :param db_adapter:
        :param sub_basin_shape_file:
        :param date_time: '2019-08-28 11:00:00'
        :return: {station1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1}, station2:{}}
        """
        available_stations = self.get_available_stations_info(date_time, exec_datetime, model, method)
        corrected_available_stations = {}
        if len(available_stations):
            for station, info in available_stations.items():
                shape_attribute = ['OBJECTID', 1]
                shape_df = gpd.GeoDataFrame.from_file(sub_basin_shape_file)
                shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
                shape_polygon = shape_df['geometry'][shape_polygon_idx]
                if Point(info['longitude'], info['latitude']).within(
                        shape_polygon):  # make a point and see if it's in the polygon
                    corrected_available_stations[station] = info
                    print('Station {} in the sub-basin'.format(station))
            return corrected_available_stations
        else:
            print('Not available stations..')
            return {}

    def get_basin_available_stations_timeseries(self, shape_file, start_time, end_time, model,
                                                method, allowed_error, exec_datetime):
        """
        Add time series to the given available station list.
        :param shape_file:
        :param hourly_csv_file_dir:
        :param adapter:
        :param start_time: '2019-08-28 11:00:00'
        :param end_time: '2019-08-28 11:00:00'
        :return: {station1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1, 'timeseries': timeseries1}, station2:{}}
        """
        basin_available_stations = self.get_available_stations_in_sub_basin(shape_file, start_time, model, method,
                                                                            exec_datetime)
        print('get_basin_available_stations_timeseries|basin_available_stations: ', basin_available_stations)
        for station in list(basin_available_stations):
            hash_id = basin_available_stations[station]['hash_id']
            station_df = self.get_timeseries_by_id(hash_id, start_time, end_time, allowed_error)
            if station_df is not None:
                basin_available_stations[station]['timeseries'] = station_df.replace(MISSING_VALUE,
                                                                                     FILL_VALUE)
            else:
                print('No times series data avaialble for the station ', station)
                basin_available_stations.pop(station, None)
        return basin_available_stations

    def get_basin_discharge(self, time, grid_id, method='SF', model='flo2d_150'):
        discharge_hash_sql = 'select id,latitude,longitude from curw_sim.dis_run where grid_id=\'{}\' ' \
                             'and method=\'{}\' and model=\'{}\';'.format(grid_id, method, model)
        print('get_basin_discharge|discharge_hash_sql : ', discharge_hash_sql)
        cursor = self.cursor
        cursor.execute(discharge_hash_sql)
        hash_result = cursor.fetchone()
        print('get_basin_discharge|hash_result : ', hash_result)
        if hash_result:
            hash_id = hash_result[0]
            print('get_basin_discharge|hash_id : ', hash_id)
            value_sql = 'select id,time,value from curw_sim.dis_data where id=\'{}\' and time =\'{}\';'.format(hash_id,
                                                                                                               time)
            cursor.execute(value_sql)
            value_result = cursor.fetchone()
            print('get_basin_discharge|value_result : ', value_result)
            if value_result:
                discharge_value = value_result[2]
                print('get_basin_discharge|value_result : ', value_result)
                return discharge_value
        return None

    def get_all_basin_stations(self, basin='kub', model='hechms', method='MME'):
        available_stations = []
        cursor = self.cursor
        try:
            sql = 'select id, grid_id, latitude, longitude from curw_sim.run where model=\'{}\' and method=\'{}\''.format(model, method)
            print('get_all_basin_stations|sql : ', sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                hash_id = row[0]
                station = row[1].split('_')[1]
                latitude = Decimal(row[2])
                longitude = Decimal(row[3])
                station_info = {'station': station, 'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}
                if is_in_basin(station_info, basin):
                    available_stations.append(station_info)
            return available_stations
        except Exception as e:
            print('get_available_stations_info|Exception:', e)
            return available_stations

    def get_timeseries_by_hash_id(self, hash_id, ts_start, ts_end, allowed_error, time_step_size=5):
        # print('get_timeseries_by_hash_id|[hash_id, ts_start, ts_end, allowed_error] : ',
        #       [hash_id, ts_start, ts_end, allowed_error])
        cursor = self.cursor
        data_sql = 'select time,value from curw_sim.data where time>=\'{}\' and time<\'{}\' ' \
                   'and id=\'{}\' '.format(ts_start, ts_end, hash_id)
        try:
            # print('get_timeseries_by_hash_id|data_sql : ', data_sql)
            cursor.execute(data_sql)
            results = cursor.fetchall()
            if len(results) > 0:
                time_step_count = int((datetime.strptime(ts_end, '%Y-%m-%d %H:%M:%S')
                                       - datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S')).total_seconds() / (
                                              60 * time_step_size))
                # print('get_timeseries_by_hash_id|time_step_count : ', time_step_count)
                missing_count = time_step_count - len(results)
                # print('get_timeseries_by_id|missing_count : ', missing_count)
                missing_count = get_null_count(results) + missing_count
                # print('get_timeseries_by_id|missing_count : ', missing_count)
                data_error = (missing_count / time_step_count)
                # print('get_timeseries_by_id|data_error : ', data_error)
                if data_error <= 0:
                    df = pd.DataFrame(data=results, columns=['time', 'value']).set_index(keys='time')
                    return df
                elif data_error > 0 and data_error <= allowed_error:
                    # print('get_timeseries_by_id|filling missing data.')
                    formatted_ts = []
                    i = 0
                    for step in range(time_step_count):
                        tms_step = datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S') + timedelta(
                            minutes=step * time_step_size)
                        if step < len(results):
                            if tms_step == results[i][0]:
                                formatted_ts.append(results[i])
                            else:
                                formatted_ts.append((tms_step, Decimal(0)))
                        else:
                            formatted_ts.append((tms_step, Decimal(0)))
                        i += 1
                    df = pd.DataFrame(data=formatted_ts, columns=['time', 'value']).set_index(keys='time')
                    return df
                else:
                    # print('get_timeseries_by_hash_id|data_error : {}'.format(data_error))
                    print('get_timeseries_by_hash_id|Data error is too large')
                    return None
            else:
                print('get_timeseries_by_hash_id|No data.')
                print('get_timeseries_by_hash_id|data_sql : ', data_sql)
                return None
        except Exception as e:
            print('get_timeseries_by_id|data fetch|Exception:', e)
            return None


class CurwFcstAdapter:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db):
        print('[mysql_user, mysql_password, mysql_host, mysql_db] : ',
              [mysql_user, mysql_password, mysql_host, mysql_db])
        try:
            self.connection = mysql.connector.connect(user=mysql_user,
                                                      password=mysql_password,
                                                      host=mysql_host,
                                                      database=mysql_db)
            self.cursor = self.connection.cursor()
        except ConnectionError as ex:
            print('ConnectionError|ex: ', ex)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_station_fcst_rainfall(self, station_ids, fcst_start, fcst_end, source=8, sim_tag='evening_18hrs'):
        """
        :param station_ids: list of station ids
        :param fcst_start:
        :param fcst_end:
        :return:{station_id:dataframe, }
        """
        fcst_ts = {}
        cursor = self.cursor
        station_ids_str = ','.join(station_ids)
        try:
            sql = 'select station as station_id, id as hash_id from curw_fcst.run where sim_tag={} and source={} ' \
                  'and station in ({}) '.format(sim_tag, source, station_ids_str)
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                for row in results:
                    station_id = row[0]
                    hash_id = row[1]
                    try:
                        sql = 'select time,value from curw_fcst.data where time>=\'{}\' and time<\'{}\' and id=\'{}\' '.format(
                            fcst_start, fcst_end, hash_id)
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        if len(results) > 0:
                            fcst_ts[station_id] = pd.DataFrame(data=results, columns=['time', 'value'])
                    except Exception as e:
                        print('Exception:', str(e))
                return fcst_ts
            else:
                return None
        except Exception as e:
            print('save_init_state|Exception:', e)
            return None
