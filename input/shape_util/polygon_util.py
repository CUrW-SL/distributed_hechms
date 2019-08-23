from resources import manager as res_mgr
from .spatial_util import get_voronoi_polygons
import pandas as pd
import os
import copy
import datetime
import numpy as np
import geopandas as gpd
from input.station_metadata import meta_data
from db_layer import get_event_id, get_time_series_values
from config import SUB_CATCHMENT_SHAPE_FILE_DIR, THESSIAN_DECIMAL_POINTS
from db_layer import MySqlAdapter
from functools import reduce
import csv

TIME_GAP_MINUTES = 5
MISSING_ERROR_PERCENTAGE = 0.3


def _voronoi_finite_polygons_2d(vor, radius=None):
    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")
    new_regions = []
    new_vertices = vor.vertices.tolist()
    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()
    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))
    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]
        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue
        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue
            # Compute the missing endpoint of an infinite ridge
            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius
            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())
        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]
        # finish
        new_regions.append(new_region.tolist())
    return new_regions, np.asarray(new_vertices)


def get_gage_points():
    gage_csv = res_mgr.get_resource_path('gages/CurwRainGauges.csv')
    gage_df = pd.read_csv(gage_csv)[['name', 'longitude', 'latitude']]
    gage_dict = gage_df.set_index('name').T.to_dict('list')
    return gage_dict


def get_thessian_polygon_from_gage_points(shape_file, gage_points):
    shape = res_mgr.get_resource_path(shape_file)
    # calculate the voronoi/thesian polygons w.r.t given station points.
    thessian_df = get_voronoi_polygons(gage_points, shape, ['OBJECTID', 1],
                                       output_shape_file=os.path.join(SUB_CATCHMENT_SHAPE_FILE_DIR,
                                                                      'sub_catchment.shp'))
    return thessian_df


def get_catchment_area(catchment_file):
    shape = res_mgr.get_resource_path(catchment_file)
    catchment_df = gpd.GeoDataFrame.from_file(shape)
    return catchment_df


def calculate_intersection(thessian_df, catchment_df):
    sub_ratios = []
    for i, catchment_polygon in enumerate(catchment_df['geometry']):
        sub_catchment_name = catchment_df.iloc[i]['Name_of_Su']
        ratio_list = []
        for j, thessian_polygon in enumerate(thessian_df['geometry']):
            if catchment_polygon.intersects(thessian_polygon):
                gage_name = thessian_df.iloc[j]['id']
                intersection = catchment_polygon.intersection(thessian_polygon)
                ratio = np.round(intersection.area / thessian_polygon.area, THESSIAN_DECIMAL_POINTS)
                ratio_dic = {'gage_name': gage_name, 'ratio': ratio}
                ratio_list.append(ratio_dic)
        # print('')
        sub_dict = {'sub_catchment_name': sub_catchment_name, 'ratios': ratio_list}
        sub_ratios.append(sub_dict)
        # print(sub_dict)
    return sub_ratios


def get_sub_catchment_rainfall(data_from, data_to, db_adapter, sub_dict, station_metadata=meta_data):
    stations_meta = copy.deepcopy(station_metadata)
    sub_catchment_name = sub_dict['sub_catchment_name']
    print('sub_catchment_name:', sub_catchment_name)
    ratio_list = sub_dict['ratios']
    for gage_dict in ratio_list:
        print('gage_dict:', gage_dict)
        gage_name = gage_dict['gage_name']
        timeseries_meta = stations_meta[gage_name]
        print('timeseries_meta:', timeseries_meta)
        try:
            db_meta_data = {'station': gage_name,
                            'variable': timeseries_meta['variable'],
                            'unit': timeseries_meta['unit'],
                            'type': timeseries_meta['event_type'],
                            'source': timeseries_meta['source'],
                            'name': timeseries_meta['run_name']}
            event_id = get_event_id(db_adapter, db_meta_data)
            print('event_id:', event_id)
            time_series_df = get_time_series_values(db_adapter, event_id, data_from, data_to)
            ratio = gage_dict['ratio']
            print('ratio:', ratio)
            time_series_df.loc[:, 'value'] *= ratio
            print('time_series_df:', time_series_df)

        except Exception as e:
            print("get_event_id|Exception|e : ", e)


def get_kub_points_from_meta_data(station_metadata=meta_data):
    kub_points = {}
    #print('station_metadata : ', type(station_metadata))
    for key, value in station_metadata.items():
        #print('key : ', key)
        #print('value : ', value)
        kub_points[key] = value['lon_lat']
    #print('kub_points : ', kub_points)
    return kub_points


def get_valid_kub_points_from_meta_data(validated_gages, station_metadata=meta_data):
    kub_points = {}
    print('station_metadata : ', type(station_metadata))
    for key, value in station_metadata.items():
        print('key : ', key)
        print('value : ', value)
        if key in validated_gages:
            kub_points[key] = value['lon_lat']
    print('kub_points : ', kub_points)
    return kub_points


def validate_gage_points(db_adapter, run_date, forward, backward, station_metadata=meta_data):
    validated_gages = {}
    for key, value in station_metadata.items():
        try:
            time_series_df = get_timeseries_data(db_adapter, run_date, forward, backward, key, value)
            if time_series_df.size > 0:
                filled_ts = fill_timeseries(run_date, forward, backward, time_series_df)
                filled_ts = filled_ts.set_index('time')
                #formatted_ts = pd.DataFrame(data=time_series_data.values, columns=['time', 'value']).set_index(keys='time')
                print(filled_ts)
                formatted_ts = filled_ts.resample('1H').sum().fillna(0)
                validated_gages[key] = formatted_ts
            else:
                print('Empty timeseries.')
        except Exception as e:
            print("validate_gage_points|Exception|e : ", e)
    return validated_gages


def fill_timeseries(run_date, forward, backward, timeseries):
    current_date = datetime.datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
    print('{current_date, forward, backward} : ', {current_date, forward, backward})
    start_date = current_date - datetime.timedelta(days=backward)
    end_date = current_date + datetime.timedelta(days=forward)
    print('current_date:', current_date)
    print('start_date:', start_date)
    print('end_date:', end_date)
    available_start = timeseries.iloc[0]['time']
    available_end = timeseries.iloc[-1]['time']
    date_ranges1 = pd.date_range(start=start_date, end=available_start, freq='15T')
    df1 = pd.DataFrame(date_ranges1, columns=['time'])
    date_ranges2 = pd.date_range(start=available_end, end=end_date, freq='15T')
    df2 = pd.DataFrame(date_ranges2, columns=['time'])
    if start_date < available_start:
        value_list = []
        i = 0
        while i < len(date_ranges1):
            value_list.append(0.00)
            i += 1
        df1['value'] = value_list
    if available_end < end_date:
        value_list = []
        i = 0
        while i < len(date_ranges2):
            value_list.append(0.00)
            i += 1
        df2['value'] = value_list
    if df1.size > 1:
        timeseries = df1.append(timeseries)
    if df2.size > 1:
        timeseries = timeseries.append(df2)
    #print(timeseries)
    return timeseries


def get_timeseries_data(db_adapter, run_date, forward, backward, key, value):
    if backward == 2 and forward == 3:
        observed_end = datetime.datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
        observed_start = observed_end - datetime.timedelta(hours=24 * backward)
        forecast_0d_start = datetime.datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
        forecast_0d_end = forecast_0d_start + datetime.timedelta(hours=24)
        forecast_1d_start = forecast_0d_end
        forecast_1d_end = forecast_1d_start + datetime.timedelta(hours=24)
        forecast_2d_start = forecast_1d_end
        forecast_2d_end = forecast_2d_start + datetime.timedelta(hours=24)
        # 'Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after'
        try:
            observed_meta_data = {'station': key,
                                  'variable': value['variable'],
                                  'unit': value['unit'],
                                  'type': value['event_type'],
                                  'source': value['source'],
                                  'name': value['run_name']}
            event_id = get_event_id(db_adapter, observed_meta_data)
            observed_ts = get_time_series_values(db_adapter, event_id, observed_start, observed_end)
            forecast_d0_meta_data = {'station': key,
                                     'variable': value['variable'],
                                     'unit': value['unit'],
                                     'type': 'Forecast-0-d',
                                     'source': 'wrf0',
                                     'name': 'Cloud-1'}
            event_id0 = get_event_id(db_adapter, forecast_d0_meta_data)
            forecast_d0_ts = get_time_series_values(db_adapter, event_id0, forecast_0d_start, forecast_0d_end)
            forecast_d1_meta_data = {'station': key,
                                     'variable': value['variable'],
                                     'unit': value['unit'],
                                     'type': 'Forecast-1-d-after',
                                     'source': 'wrf0',
                                     'name': 'Cloud-1'}
            event_id1 = get_event_id(db_adapter, forecast_d1_meta_data)
            forecast_d1_ts = get_time_series_values(db_adapter, event_id1, forecast_1d_start, forecast_1d_end)
            forecast_d2_meta_data = {'station': key,
                                     'variable': value['variable'],
                                     'unit': value['unit'],
                                     'type': 'Forecast-2-d-after',
                                     'source': 'wrf0',
                                     'name': 'Cloud-1'}
            event_id2 = get_event_id(db_adapter, forecast_d2_meta_data)
            forecast_d2_ts = get_time_series_values(db_adapter, event_id2, forecast_2d_start, forecast_2d_end)
            total_forecast_ts = pd.concat([forecast_d0_ts, forecast_d1_ts, forecast_d2_ts])

            final_ts = pd.concat([observed_ts, total_forecast_ts])
            return final_ts
        except Exception as ex:
            print('get_timeseries_data|Exception|ex:', ex)


def get_forecasted_ts_data(db_adapter, run_date, forward, key, value):
    if forward == 3:
        forecast_0d_start = datetime.datetime.strptime(run_date, '%Y-%m-%d %H:%M:%S')
        forecast_0d_end = forecast_0d_start + datetime.timedelta(hours=24)
        forecast_1d_start = forecast_0d_end
        forecast_1d_end = forecast_1d_start + datetime.timedelta(hours=24)
        forecast_2d_start = forecast_1d_end
        forecast_2d_end = forecast_2d_start + datetime.timedelta(hours=24)
        try:
            db_meta_data = {'station': key,
                            'variable': value['variable'],
                            'unit': value['unit'],
                            'type': value['event_type'],
                            'source': value['source'],
                            'name': value['run_name']}
            event_id0 = get_event_id(db_adapter, db_meta_data)
            forecast_d0_ts = get_time_series_values(db_adapter, event_id0, forecast_0d_start, forecast_0d_end)
            db_meta_data = {'station': key,
                            'variable': value['variable'],
                            'unit': value['unit'],
                            'type': value['event_type'],
                            'source': value['source'],
                            'name': value['run_name']}
            event_id1 = get_event_id(db_adapter, db_meta_data)
            forecast_d1_ts = get_time_series_values(db_adapter, event_id1, forecast_1d_start, forecast_1d_end)
            db_meta_data = {'station': key,
                            'variable': value['variable'],
                            'unit': value['unit'],
                            'type': value['event_type'],
                            'source': value['source'],
                            'name': value['run_name']}
            event_id2 = get_event_id(db_adapter, db_meta_data)
            forecast_d2_ts = get_time_series_values(db_adapter, event_id2, forecast_2d_start, forecast_2d_end)
            total_forecast_ts = forecast_d0_ts + forecast_d1_ts + forecast_d2_ts
            #print('total_forecast_ts : ', total_forecast_ts)
            return total_forecast_ts
        except Exception as ex:
            print("get_forecasted_ts_data|Exception|e : ", ex)


def get_rain_files(file_name, run_datetime, forward=3, backward=2):
    print('get_rain_files|{run_datetime, forward, backward}: ', {run_datetime, forward, backward})
    db_adapter = MySqlAdapter()
    valid_gages = validate_gage_points(db_adapter, run_datetime, forward, backward)
    print('valid_gages.keys() : ', valid_gages.keys())
    kub_points = get_valid_kub_points_from_meta_data(valid_gages)
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'sub_catchments/Hasitha_subcatchments.shp'
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        catchments_list = []
        catchments_rf_df_list = []
        for sub_dict in sub_ratios:
            ratio_list = sub_dict['ratios']
            sub_catchment_name = sub_dict['sub_catchment_name']
            gage_dict = ratio_list[0]
            gage_name = gage_dict['gage_name']
            sub_catchment_df = valid_gages[gage_name]
            ratio = gage_dict['ratio']
            if ratio > 0:
                sub_catchment_df.loc[:, 'value'] *= ratio
            ratio_list.remove(gage_dict)
            for gage_dict in ratio_list:
                gage_name = gage_dict['gage_name']
                time_series_df = valid_gages[gage_name]
                ratio = gage_dict['ratio']
                time_series_df.loc[:, 'value'] *= ratio
                sub_catchment_df['value'] = sub_catchment_df['value'] + time_series_df['value']
            if sub_catchment_df.size > 0:
                catchments_list.append(sub_catchment_name)
                catchments_rf_df_list.append(sub_catchment_df)
        MySqlAdapter.close_connection(db_adapter)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['time'],
                                                        how='outer'), catchments_rf_df_list)
        df_merged.to_csv('df_merged.csv', header=False)
        file_handler = open(file_name, 'w')
        csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
        # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
        first_row = ['Location Names']
        first_row.extend(catchments_list)
        second_row = ['Location Ids']
        second_row.extend(catchments_list)
        third_row = ['Time']
        for i in range(len(catchments_list)):
            third_row.append('Rainfall')
        csvWriter.writerow(first_row)
        csvWriter.writerow(second_row)
        csvWriter.writerow(third_row)
        file_handler.close()
        df_merged.to_csv(file_name, mode='a', header=False)
    except Exception as e:
        MySqlAdapter.close_connection(db_adapter)
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)


def get_sub_catchment_rain_files(file_name, from_datetime, to_datetime):
    print('get_sub_catchment_rain_files|{from_datetime, to_datetime}: ', {from_datetime, to_datetime})
    db_adapter = MySqlAdapter()
    valid_gages = validate_gage_points(db_adapter, from_datetime, to_datetime)
    print('valid_gages : ', valid_gages)
    print('valid_gages.keys() : ', valid_gages.keys())
    kub_points = get_valid_kub_points_from_meta_data(valid_gages)
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        # catchment_file = 'kub/sub_catchments/sub_catchments1.shp'
        catchment_file = 'sub_catchments/Hasitha_subcatchments.shp'
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        catchments_list = []
        catchments_rf_df_list = []
        for sub_dict in sub_ratios:
            ratio_list = sub_dict['ratios']
            sub_catchment_name = sub_dict['sub_catchment_name']
            gage_dict = ratio_list[0]
            print('gage_dict:', gage_dict)
            gage_name = gage_dict['gage_name']
            sub_catchment_df = valid_gages[gage_name]
            ratio = gage_dict['ratio']
            print('ratio:', ratio)
            sub_catchment_df.loc[:, 'value'] *= ratio
            ratio_list.remove(gage_dict)
            for gage_dict in ratio_list:
                print('gage_dict:', gage_dict)
                gage_name = gage_dict['gage_name']
                time_series_df = valid_gages[gage_name]
                ratio = gage_dict['ratio']
                print('ratio:', ratio)
                time_series_df.loc[:, 'value'] *= ratio
                sub_catchment_df['value'] = sub_catchment_df['value'] + time_series_df['value']
            catchments_list.append(sub_catchment_name)
            catchments_rf_df_list.append(sub_catchment_df)
        MySqlAdapter.close_connection(db_adapter)
        print("catchments_list : ", catchments_list)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['time'],
                                                        how='outer'), catchments_rf_df_list)
        print('df_merged : ', df_merged)
        print('df_merged.columns : ', df_merged.columns.values)
        df_merged.to_csv('df_merged.csv', header=False)
        file_handler = open(file_name, 'w')
        csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
        # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
        first_row = ['Location Names']
        first_row.extend(catchments_list)
        second_row = ['Location Ids']
        second_row.extend(catchments_list)
        third_row = ['Time']
        for i in range(len(catchments_list)):
            third_row.append('Rainfall')
        csvWriter.writerow(first_row)
        csvWriter.writerow(second_row)
        csvWriter.writerow(third_row)
        file_handler.close()
        df_merged.to_csv(file_name, mode='a', header=False)
    except Exception as e:
        MySqlAdapter.close_connection(db_adapter)
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)


def get_valid_gages(start_date, end_date):
    db_adapter = MySqlAdapter()
    valid_gages = validate_gage_points(db_adapter, start_date, end_date)
    MySqlAdapter.close_connection(db_adapter)
    print(valid_gages.keys())
    return valid_gages


def get_sub_ratios():
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'kub/sub_catchments/sub_catchments1.shp'
        kub_points = get_kub_points_from_meta_data()
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        return sub_ratios
    except Exception as e:
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)


def get_timeseris():
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'kub/sub_catchments/sub_catchments1.shp'
        kub_points = get_kub_points_from_meta_data()
        thessian_df = get_thessian_polygon_from_gage_points(shape_file, kub_points)
        catchment_df = get_catchment_area(catchment_file)
        sub_ratios = calculate_intersection(thessian_df, catchment_df)
        print(sub_ratios)
        db_adapter = MySqlAdapter()
        get_sub_catchment_rainfall('2018-09-27 00:00:00', '2018-09-30 00:00:00', db_adapter, sub_ratios[1])
        MySqlAdapter.close_connection(db_adapter)
    except Exception as e:
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)
