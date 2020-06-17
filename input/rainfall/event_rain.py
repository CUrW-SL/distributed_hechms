import csv
import os
from decimal import Decimal
import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point
from db_layer import CurwSimAdapter
from functools import reduce
from datetime import datetime, timedelta
from config import RESOURCE_PATH


THESSIAN_DECIMAL_POINTS = 4
MISSING_VALUE = -99999
FILL_VALUE = 0


def get_ts_for_start_end(sim_adapter, all_stations, ts_start, ts_end):
    formatted_stations = []
    for station_info in all_stations:
        hash_id = station_info['hash_id']
        tms_df = sim_adapter.get_timeseries_by_hash_id(hash_id, ts_start, ts_end, allowed_error=0.2, time_step_size=5)
        if tms_df is not None:
            # print('get_ts_for_start_end|tms_df : ', tms_df)
            if tms_df is not None:
                station_info['tms_df'] = tms_df.replace(MISSING_VALUE,
                                                        FILL_VALUE)
                formatted_stations.append(station_info)
    return formatted_stations


def create_hl_df(ts_start_str, ts_end_str):
    time_series = []
    ts_start = datetime.strptime(ts_start_str, '%Y-%m-%d %H:%M:%S')
    ts_end = datetime.strptime(ts_end_str, '%Y-%m-%d %H:%M:%S')
    ts_step = ts_start
    while ts_step < ts_end:
        next_ts_step = ts_step + timedelta(minutes=5)
        time_series.append({'Time': ts_step.strftime('%Y-%m-%d %H:%M:%S'),
                            'Rainfall': Decimal(0.0)})
        ts_step = next_ts_step
    mean_rain_df = pd.DataFrame(data=time_series, columns=['Time', 'Rain']).set_index('Time', inplace=True)
    # print('create_hl_df|mean_rain_df : ', mean_rain_df)
    return mean_rain_df


def create_df(ts_start_str, ts_end_str):
    time_series = []
    ts_start = datetime.strptime(ts_start_str, '%Y-%m-%d %H:%M:%S')
    ts_end = datetime.strptime(ts_end_str, '%Y-%m-%d %H:%M:%S')
    ts_step = ts_start
    while ts_step < ts_end:
        next_ts_step = ts_step + timedelta(minutes=5)
        time_series.append({'Time': ts_step.strftime('%Y-%m-%d %H:%M:%S'),
                            'Rainfall1': Decimal(0.0),
                            'Rainfall2': Decimal(0.0),
                            'Rainfall3': Decimal(0.0),
                            'Rainfall4': Decimal(0.0),
                            'Rainfall5': Decimal(0.0)})
        ts_step = next_ts_step
    mean_rain_df = pd.DataFrame(data=time_series,
                                columns=['Time', 'Rainfall1', 'Rainfall2', 'Rainfall3', 'Rainfall4', 'Rainfall5']).set_index(keys='Time')
    # print('create_df|mean_rain_df : ', mean_rain_df)
    return mean_rain_df


def get_basin_rain(ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime,
                   db_user, db_pwd, db_host, db_name='curw_sim', target_model='HDC', catchment='kub'):
    print('[ts_start, ts_end, output_dir, model, pop_method, allowed_error, exec_datetime, target_model, catchment] : ',
          [ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, target_model,
           catchment])
    if target_model == 'HDC' or target_model == 'HDE':
        get_hd_mean_rain(ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, db_user,
                         db_pwd, db_host, db_name, catchment)
    else:
        get_hl_mean_rain(ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, db_user,
                         db_pwd, db_host, db_name, catchment)


def get_hl_mean_rain(ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, db_user,
                     db_pwd, db_host, db_name, catchment):
    sim_adapter = None
    print('get_hl_mean_rain|[ts_start, ts_end, output_dir, model, pop_method, allowed_error, exec_datetime] : ',
          [ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime])
    try:
        basin_shape_file = os.path.join(RESOURCE_PATH, 'total_catchment/Glen_Tot_Catchment.shp')
        sim_adapter = CurwSimAdapter(db_user, db_pwd, db_host, db_name)
        all_stations = sim_adapter.get_all_basin_stations()
        # [{'station': station, 'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}]
        print('get_basin_rain|all_stations : ', all_stations)
        ts_start = datetime.strptime(ts_start_str, '%Y-%m-%d %H:%M:%S')
        ts_end = datetime.strptime(ts_end_str, '%Y-%m-%d %H:%M:%S')
        ts_step = ts_start
        step_one = True
        output_file = os.path.join(output_dir, 'DailyRain.csv')
        while ts_step < ts_end:
            next_ts_step = ts_step + timedelta(minutes=60)
            ts_start_str = ts_step.strftime('%Y-%m-%d %H:%M:%S')
            ts_end_str = next_ts_step.strftime('%Y-%m-%d %H:%M:%S')
            all_stations_tms = get_ts_for_start_end(sim_adapter, all_stations, ts_start_str, ts_end_str)
            zero_tms_df = create_hl_df(ts_start_str, ts_end_str)
            calculate_hl_step_mean(basin_shape_file, all_stations_tms, output_file, step_one, zero_tms_df)
            step_one = False
            ts_step = next_ts_step
        file_handler = open(output_file, 'a')
        csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
        csvWriter.writerow([ts_end, 0.0])
        file_handler.close()
        sim_adapter.close_connection()
    except Exception as e:
        if sim_adapter is not None:
            sim_adapter.close_connection()
        print('get_hl_mean_rain|Exception : ', str(e))


def calculate_hl_step_mean(basin_shape_file, station_infos, output_file, step_one, zero_tms_df):
    print('calculate_hl_step_mean|[basin_shape_file, station_infos] : ', [basin_shape_file, station_infos])
    try:
        gauge_points = {}
        for station_info in station_infos:
            station = station_info['station']
            gauge_points[station] = ['%.6f' % station_info['longitude'], '%.6f' % station_info['latitude']]
        # print('calculate_step_mean|gauge_points : ', gauge_points)
        gauge_points_thessian = get_thessian_polygon_from_gage_points(basin_shape_file, gauge_points)
        # print('calculate_step_mean|gauge_points_thessian : ', gauge_points_thessian)
        catchment_df = gpd.GeoDataFrame.from_file(basin_shape_file)
        sub_ratios = hl_calculate_intersection(gauge_points_thessian, catchment_df)
        # print('calculate_step_mean|sub_ratios : ', sub_ratios)
        catchment_rain = []
        catchment_name_list = []
        for sub_ratio in sub_ratios:
            catchment_name = sub_ratio['sub_catchment_name']
            catchment_ts_list = []
            ratios = sub_ratio['ratios']
            for ratio in ratios:
                gauge_name = ratio['gage_name']
                ratio = Decimal(ratio['ratio'])
                gauge_info = next((sub for sub in station_infos if sub['station'] == gauge_name), None)
                if gauge_info is not None:
                    gauge_ts = gauge_info['tms_df']
                    modified_gauge_ts = gauge_ts.multiply(ratio, axis='value')
                    catchment_ts_list.append(modified_gauge_ts)
            total_rain = reduce(lambda x, y: x.add(y, fill_value=0), catchment_ts_list)
            total_rain.rename(columns={'value': catchment_name}, inplace=True)
            catchment_name_list.append(catchment_name)
            catchment_rain.append(total_rain)
        if len(catchment_rain) > 0:
            mean_rain = catchment_rain[0].join(catchment_rain[1:])
        else:
            mean_rain = zero_tms_df
        _write_mean_rain_to_file(mean_rain, output_file, catchment_name_list, step_one)
    except Exception as ex:
        print('calculate_hl_step_mean|Exception : ', str(ex))


def get_hd_mean_rain(ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, db_user,
                     db_pwd, db_host, db_name, catchment):
    sim_adapter = None
    try:
        print('get_hd_mean_rain|[ts_start, ts_end, output_dir, model, pop_method, allowed_error, exec_datetime, catchment] : ',
              [ts_start_str, ts_end_str, output_dir, model, pop_method, allowed_error, exec_datetime, catchment])
        sub_catchment_shape_file = os.path.join(RESOURCE_PATH, 'sub_catchments/sub_subcatchments.shp')
        if catchment == 'kub':
            shape_file = os.path.join(RESOURCE_PATH, 'kub-wgs84/kub-wgs84.shp')
        else:
            shape_file = os.path.join(RESOURCE_PATH, 'klb-wgs84/klb-wgs84.shp')
        sim_adapter = CurwSimAdapter(db_user, db_pwd, db_host, db_name)
        all_stations = sim_adapter.get_all_basin_stations()
        # [{'station': station, 'hash_id': hash_id, 'latitude': latitude, 'longitude': longitude}]
        # print('get_basin_rain|all_stations : ', all_stations)
        ts_start = datetime.strptime(ts_start_str, '%Y-%m-%d %H:%M:%S')
        ts_end = datetime.strptime(ts_end_str, '%Y-%m-%d %H:%M:%S')
        ts_step = ts_start
        step_one = True
        output_file = os.path.join(output_dir, 'DailyRain.csv')
        while ts_step < ts_end:
            next_ts_step = ts_step + timedelta(minutes=60)
            ts_start_str = ts_step.strftime('%Y-%m-%d %H:%M:%S')
            ts_end_str = next_ts_step.strftime('%Y-%m-%d %H:%M:%S')
            all_stations_tms = get_ts_for_start_end(sim_adapter, all_stations, ts_start_str, ts_end_str)
            zero_tms_df = create_df(ts_start_str, ts_end_str)
            calculate_hd_step_mean(shape_file, sub_catchment_shape_file, all_stations_tms,
                                   output_file, step_one, zero_tms_df)
            step_one = False
            ts_step = next_ts_step
        file_handler = open(output_file, 'a')
        csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
        csvWriter.writerow([ts_end, 0.0, 0.0, 0.0, 0.0, 0.0])
        file_handler.close()
        sim_adapter.close_connection()
    except Exception as e:
        if sim_adapter is not None:
            sim_adapter.close_connection()
        print('get_hd_mean_rain|Exception : ', str(e))


def calculate_hd_step_mean(shape_file, sub_catchment_shape_file, station_infos, output_file, step_one, zero_tms_df):
    try:
        # print('calculate_hd_step_mean|station_infos : ', station_infos)
        gauge_points = {}
        for station_info in station_infos:
            station = station_info['station']
            gauge_points[station] = ['%.6f' % station_info['longitude'], '%.6f' % station_info['latitude']]
        catchment_rain = []
        catchment_name_list = []
        print('calculate_hd_step_mean|gauge_points : ', gauge_points)
        if gauge_points:  ## TODO: check on empty gauge points
            gauge_points_thessian = get_thessian_polygon_from_gage_points(shape_file, gauge_points)
            # print('calculate_hd_step_mean|gauge_points_thessian : ', gauge_points_thessian)
            catchment_df = gpd.GeoDataFrame.from_file(sub_catchment_shape_file)
            # print('calculate_hd_step_mean|catchment_df : ', catchment_df)
            print('calculate_hd_step_mean|calculating sub ratios')
            sub_ratios = calculate_intersection(gauge_points_thessian, catchment_df)
            print('calculate_hd_step_mean|sub_ratios : ', sub_ratios)
            for sub_ratio in sub_ratios:
                catchment_name = sub_ratio['sub_catchment_name']
                catchment_ts_list = []
                ratios = sub_ratio['ratios']
                for ratio in ratios:
                    gauge_name = ratio['gage_name']
                    ratio = Decimal(ratio['ratio'])
                    gauge_info = next((sub for sub in station_infos if sub['station'] == gauge_name), None)
                    if gauge_info is not None:
                        gauge_ts = gauge_info['tms_df']
                        modified_gauge_ts = gauge_ts.multiply(ratio, axis='value')
                        catchment_ts_list.append(modified_gauge_ts)
                total_rain = reduce(lambda x, y: x.add(y, fill_value=0), catchment_ts_list)
                total_rain.rename(columns={'value': catchment_name}, inplace=True)
                catchment_name_list.append(catchment_name)
                catchment_rain.append(total_rain)
        print('calculate_hd_step_mean|len(catchment_rain) : ', len(catchment_rain))
        if len(catchment_rain) > 0:
            print('calculate_hd_step_mean|Rain data')
            mean_rain = catchment_rain[0].join(catchment_rain[1:])
        else:
            print('calculate_hd_step_mean|No Rain data')
            mean_rain = zero_tms_df
        _write_mean_rain_to_file(mean_rain, output_file, catchment_name_list, step_one)
    except Exception as e:
        print('calculate_hd_step_mean|Exception : ', str(e))


def _write_mean_rain_to_file(mean_rain, output_file, catchment_name_list, step_one):
    try:
        if step_one:
            file_handler = open(output_file, 'w')
            csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
            first_row = ['Location Names']
            first_row.extend(catchment_name_list)
            second_row = ['Location Ids']
            second_row.extend(catchment_name_list)
            third_row = ['Time']
            for i in range(len(catchment_name_list)):
                third_row.append('Rainfall')
            csvWriter.writerow(first_row)
            csvWriter.writerow(second_row)
            csvWriter.writerow(third_row)
            file_handler.close()
            mean_rain.to_csv(output_file, mode='a', header=False)
        else:
            mean_rain.to_csv(output_file, mode='a', header=False)
    except Exception as ex:
        print('_write_mean_rain_to_file|Exception: ', str(ex))


def calculate_intersection(thessian_df, catchment_df):
    # print('calculate_intersection|thessian_df : ', thessian_df)
    # print('calculate_intersection|catchment_df : ', catchment_df)
    sub_ratios = []
    for i, catchment_polygon in enumerate(catchment_df['geometry']):
        sub_catchment_name = catchment_df.iloc[i]['Name_of_Su']
        ratio_list = []
        for j, thessian_polygon in enumerate(thessian_df['geometry']):
            if catchment_polygon.intersects(thessian_polygon):
                gage_name = thessian_df.iloc[j]['id']
                intersection = catchment_polygon.intersection(thessian_polygon)
                ratio = np.round(intersection.area / catchment_polygon.area, THESSIAN_DECIMAL_POINTS)
                ratio_dic = {'gage_name': gage_name, 'ratio': ratio}
                ratio_list.append(ratio_dic)
        sub_dict = {'sub_catchment_name': sub_catchment_name, 'ratios': ratio_list}
        sub_ratios.append(sub_dict)
    return sub_ratios


def hl_calculate_intersection(thessian_df, catchment_df):
    sub_ratios = []
    for i, catchment_polygon in enumerate(catchment_df['geometry']):
        sub_catchment_name = catchment_df.iloc[i]['Name_of_Su']
        ratio_list = []
        for j, thessian_polygon in enumerate(thessian_df['geometry']):
            if catchment_polygon.intersects(thessian_polygon):
                gage_name = thessian_df.iloc[j]['id']
                intersection = catchment_polygon.intersection(thessian_polygon)
                ratio = np.round(intersection.area / catchment_polygon.area, THESSIAN_DECIMAL_POINTS)
                ratio_dic = {'gage_name': gage_name, 'ratio': ratio}
                ratio_list.append(ratio_dic)
        sub_dict = {'sub_catchment_name': sub_catchment_name, 'ratios': ratio_list}
        sub_ratios.append(sub_dict)
    return sub_ratios


def get_thessian_polygon_from_gage_points(shape_file, gage_points):
    # shape = res_mgr.get_resource_path(shape_file)
    # calculate the voronoi/thesian polygons w.r.t given station points.
    print('get_thessian_polygon_from_gage_points|shape_file : ', shape_file)
    voronoi_polygon = get_voronoi_polygons(gage_points, shape_file, ['OBJECTID', 1])
    print('get_thessian_polygon_from_gage_points|voronoi_polygon : ', voronoi_polygon)
    return voronoi_polygon


def get_voronoi_polygons(points_dict, shape_file, shape_attribute=None, output_shape_file=None, add_total_area=True):
    """
    :param points_dict: dict of points {'id' --> [lon, lat]}
    :param shape_file: shape file path of the area
    :param shape_attribute: attribute list of the interested region [key, value]
    :param output_shape_file: if not none, a shape file will be created with the output
    :param add_total_area: if true, total area shape will also be added to output
    :return:
    geo_dataframe with voronoi polygons with columns ['id', 'lon', 'lat','area', 'geometry'] with last row being the area of the
    shape file
    """
    if shape_attribute is None:
        shape_attribute = ['OBJECTID', 1]

    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]

    ids = [p if type(p) == str else np.asscalar(p) for p in points_dict.keys()]
    points = np.array(list(points_dict.values()))[:, :2]
    vor = Voronoi(points)

    regions, vertices = _voronoi_finite_polygons_2d(vor)

    data = []
    for i, region in enumerate(regions):
        polygon = Polygon([tuple(x) for x in vertices[region]])
        if polygon.intersects(shape_polygon):
            intersection = polygon.intersection(shape_polygon)
            data.append({'id': ids[i], 'lon': vor.points[i][0], 'lat': vor.points[i][1], 'area': intersection.area,
                         'geometry': intersection
                         })
    df = gpd.GeoDataFrame(data, columns=['id', 'lon', 'lat', 'area', 'geometry'], crs=shape_df.crs)
    if output_shape_file is not None:
        df.to_file(output_shape_file)
    return df


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.
    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.
    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of input vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram
    """
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


def get_basin_init_discharge(init_date_time, db_user, db_pwd, db_host, db_name='curw_sim'):
    # print('get_basin_init_discharge|init_date_time : ', init_date_time)
    sim_adapter = CurwSimAdapter(db_user, db_pwd, db_host, db_name)
    value = sim_adapter.get_basin_discharge(init_date_time, grid_id='discharge_glencourse')
    # print('get_basin_init_discharge|value : ', value)
    return value


if __name__ == '__main__':
    try:
        db_host = "35.197.98.125"
        db_user = "admin"
        db_pwd = "floody"
        MYSQL_DB = "curw_sim"
        ts_start = '2020-05-29 00:00:00'
        ts_end = '2020-05-30 00:00:00'
        exec_date = '2020-06-17 06:00:00'
        output_dir = '/home/hasitha/PycharmProjects/distributed_hechms/output/'
        output_dir = os.path.join(output_dir,exec_date)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        get_basin_rain(ts_start, ts_end, output_dir, 'hechms', 'MME', 0.8, exec_date,
                       db_user, db_pwd, db_host, db_name='curw_sim', catchment='kub', target_model='HDC')
    except Exception as e:
        print('Exception: ', str(e))
