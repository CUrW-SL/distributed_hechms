import decimal

from db_layer import CurwSimAdapter
from resources import manager as res_mgr
from .spatial_util import get_voronoi_polygons
import pandas as pd
import os
import copy
import datetime
import numpy as np
import geopandas as gpd
from input.station_metadata import meta_data_observed as meta_data
from config import SUB_CATCHMENT_SHAPE_FILE_DIR, THESSIAN_DECIMAL_POINTS, \
                        MYSQL_USER, MYSQL_DB, MYSQL_HOST, MYSQL_PASSWORD
from functools import reduce
import csv

TIME_GAP_MINUTES = 5
MISSING_ERROR_PERCENTAGE = 0.3


def validate_gage_points(sim_adapter, ts_start, ts_end, station_metadata=meta_data):
    validated_gages = {}
    for key, value in station_metadata.items():
        try:
            time_series_df = sim_adapter.get_station_timeseries(ts_start, ts_end, key, value['run_name'])
            if time_series_df.size > 0:
                filled_ts = fill_timeseries(ts_start, ts_end, time_series_df)
                filled_ts = filled_ts.set_index('time')
                formatted_ts = filled_ts.resample('1H').sum().fillna(0)
                validated_gages[key] = formatted_ts
            else:
                print('')
        except Exception as e:
            print('')
    return validated_gages


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


def validate_gage_points(sim_adapter, ts_start, ts_end, station_metadata=meta_data):
    validated_gages = {}
    for key, value in station_metadata.items():
        try:
            time_series_df = sim_adapter.get_station_timeseries(ts_start, ts_end, key, value['run_name'])
            print('time_series_df : ', time_series_df)
            if time_series_df is not None:
                if time_series_df.size > 0:
                    validated_gages[key] = time_series_df
            else:
                print('Empty timeseries.')
        except Exception as e:
            print("validate_gage_points|Exception|e : ", e)
    return validated_gages


def fill_timeseries(ts_start, ts_end, timeseries):
    start_date = datetime.datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(ts_end, '%Y-%m-%d %H:%M:%S')
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


def get_rain_files(file_name, ts_start, ts_end):
    print('get_rain_files|{file_name, ts_start, ts_end}: ', {file_name, ts_start, ts_end})
    sim_adapter = CurwSimAdapter(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DB)
    valid_gages = validate_gage_points(sim_adapter, ts_start, ts_end)
    print('valid_gages.keys() : ', valid_gages.keys())
    kub_points = get_valid_kub_points_from_meta_data(valid_gages)
    try:
        shape_file = 'kub-wgs84/kub-wgs84.shp'
        catchment_file = 'sub_catchments/sub_catchments.shp'
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
                sub_catchment_df.loc[:, 'value'] *= decimal.Decimal(ratio)
            ratio_list.remove(gage_dict)
            for gage_dict in ratio_list:
                gage_name = gage_dict['gage_name']
                time_series_df = valid_gages[gage_name]
                ratio = gage_dict['ratio']
                time_series_df.loc[:, 'value'] *= decimal.Decimal(ratio)
                sub_catchment_df['value'] = sub_catchment_df['value'] + time_series_df['value']
            if sub_catchment_df.size > 0:
                catchments_list.append(sub_catchment_name)
                catchments_rf_df_list.append(sub_catchment_df)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['time'],
                                                        how='outer'), catchments_rf_df_list)
        print('df_merged : ', df_merged)
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
        print("get_thessian_polygon_from_gage_points|Exception|e : ", e)
