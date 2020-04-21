import csv
import os
from decimal import Decimal
import geopandas as gpd
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point
from functools import reduce
import pymysql
from datetime import datetime, timedelta

# from weather_models.hechms.rainfall.db_plugin import get_basin_available_stations_timeseries
from input.event_rain.db_plugin import get_basin_available_stations_timeseries

THESSIAN_DECIMAL_POINTS = 4

# RESOURCE_PATH = '/home/hasitha/PycharmProjects/DSS-Framework/resources/shape_files'
RESOURCE_PATH = '/home/curw/git/distributed_hechms/resources'

# connection params
HOST = "35.227.163.211"
USER = "admin"
PASSWORD = "floody"
SIM_DB = "curw_sim"
FCST_DB = "curw_fcst"
OBS_DB = "curw_obs"
PORT = 3306


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


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_thessian_polygon_from_gage_points(output_dir, shape_file, gage_points):
    # shape = res_mgr.get_resource_path(shape_file)
    # calculate the voronoi/thesian polygons w.r.t given station points.
    output_shape_path = os.path.join(output_dir, 'shape')
    create_dir_if_not_exists(output_shape_path)
    output_shape_file = os.path.join(output_shape_path, 'output.shp')
    voronoi_polygon = get_voronoi_polygons(gage_points, shape_file, ['OBJECTID', 1],
                                           output_shape_file=output_shape_file)
    print('voronoi_polygon : ', voronoi_polygon)
    return voronoi_polygon


def calculate_intersection(thessian_df, catchment_df):
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


def get_event_mean_rain(run_datetime, forward, backward, output_dir, wrf_model, sim_tag='dwrf_gfs_d1_18'):
    try:
        print('[run_datetime, forward, backward, output_dir, sim_tag] : ', [run_datetime, forward, backward, output_dir, sim_tag])
        date_limits = get_ts_start_end(run_datetime, forward, backward)
        print('get_mean_rain|date_limits : ', date_limits)
        sim_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=SIM_DB,
                                         cursorclass=pymysql.cursors.DictCursor)
        fcst_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=FCST_DB,
                                          cursorclass=pymysql.cursors.DictCursor)
        obs_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=OBS_DB,
                                         cursorclass=pymysql.cursors.DictCursor)
        shape_file = os.path.join(RESOURCE_PATH, 'kub-wgs84/kub-wgs84.shp')
        # {station1:{'hash_id': hash_id1, 'latitude': latitude1, 'longitude': longitude1, 'timeseries': timeseries1}}
        available_stations = get_basin_available_stations_timeseries(obs_connection, fcst_connection, sim_connection, shape_file,
                                                date_limits, sim_tag, wrf_model, max_error=0.3)
        # {'id' --> [lon, lat]}
        gauge_points = {}
        for station, info in available_stations.items():
            print('Final available station : ', station)
            gauge_points[station] = ['%.6f' % info['longitude'], '%.6f' % info['latitude']]
        print('gauge_points : ', gauge_points)
        print('output_dir : ', output_dir)
        gauge_points_thessian = get_thessian_polygon_from_gage_points(output_dir, shape_file, gauge_points)
        print('gauge_points_thessian : ', gauge_points_thessian)
        # shape_file = res_mgr.get_resource_path(os.path.join(RESOURCE_PATH, 'sub_catchments/sub_catchments.shp'))
        shape_file = os.path.join(RESOURCE_PATH, 'sub_catchments/sub_subcatchments.shp')
        catchment_df = gpd.GeoDataFrame.from_file(shape_file)
        sub_ratios = calculate_intersection(gauge_points_thessian, catchment_df)
        print('sub_ratios : ', sub_ratios)
        catchment_rain = []
        catchment_name_list = []
        for sub_ratio in sub_ratios:
            catchment_name = sub_ratio['sub_catchment_name']
            catchment_ts_list = []
            ratios = sub_ratio['ratios']
            for ratio in ratios:
                # {'gage_name': 'Dickoya', 'ratio': 0.9878}
                gauge_name = ratio['gage_name']
                ratio = Decimal(ratio['ratio'])
                gauge_ts = available_stations[gauge_name]['timeseries']
                print('get_event_mean_rain|gauge_ts : ', gauge_ts)
                gauge_ts.to_csv(os.path.join(output_dir, '{}_{}_rain.csv'.format(catchment_name, gauge_name)))
                modified_gauge_ts = gauge_ts.multiply(ratio, axis='value')
                modified_gauge_ts.to_csv(os.path.join(output_dir,
                                                      '{}_{}_ratio_rain.csv'.format(catchment_name, gauge_name)))
                catchment_ts_list.append(modified_gauge_ts)
            total_rain = reduce(lambda x, y: x.add(y, fill_value=0), catchment_ts_list)
            total_rain.rename(columns={'value': catchment_name}, inplace=True)
            catchment_name_list.append(catchment_name)
            catchment_rain.append(total_rain)
        if len(catchment_rain) >= 1:
            mean_rain = catchment_rain[0].join(catchment_rain[1:])
            output_file = os.path.join(output_dir, 'DailyRain.csv')
            file_handler = open(output_file, 'w')
            csvWriter = csv.writer(file_handler, delimiter=',', quotechar='|')
            # Write Metadata https://publicwiki.deltares.nl/display/FEWSDOC/CSV
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
        sim_connection.close()
        fcst_connection.close()
        obs_connection.close()
    except Exception as e:
        print("get_mean_rain|Exception|e : ", e)


def get_ts_start_end(run_datetime, forward=3, backward=2):
    result = []
    """
    method for geting timeseries start and end using input params.
    :param run_date:run_date: string yyyy-mm-ddd
    :param run_time:run_time: string hh:mm:ss
    :param forward:int
    :param backward:int
    :return: tuple (string, string)
    """
    run_datetime_tmp = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    print('get_ts_start_end|run_datetime_tmp : ', run_datetime_tmp)
    run_date = run_datetime_tmp.strftime('%Y-%m-%d')
    run_datetime_tmp = datetime.strptime('%s %s' % (run_date, '00:00:00'), '%Y-%m-%d %H:%M:%S')
    ts_start_datetime = run_datetime_tmp - timedelta(days=backward)
    ts_end_datetime = run_datetime_tmp + timedelta(days=forward)
    result.append(ts_start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    result.append(run_datetime)
    result.append(ts_end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print(result)
    return result


if __name__ == '__main__':
    try:
        output_dir = '/home/hasitha/PycharmProjects/DSS-Framework/output/hechms'
        run_datetime = '2020-03-10 08:00:00'
        forward = 3
        backward = 2
        wrf_model = 19
        get_event_mean_rain(run_datetime, forward, backward, output_dir, wrf_model, sim_tag='dwrf_gfs_d1_18')
    except Exception as e:
        print(str(e))

