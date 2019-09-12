from resources import manager as res_mgr
import geopandas as gpd


if __name__ == '__main__':
    shape_file = res_mgr.get_resource_path('kub-wgs84/kub-wgs84.shp')
    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    print('shape_df : ', shape_df)
    shape_polygon = shape_df['geometry']
    print('shape_polygon : ', shape_df['geometry'])
    area = shape_polygon.area
    print('kub area : ', area)
    sub_shape_file = res_mgr.get_resource_path('sub_catchments/sub_subcatchments.shp')
    catchment_df = gpd.GeoDataFrame.from_file(sub_shape_file)
    for i, catchment_polygon in enumerate(catchment_df['geometry']):
        print('sub area : ', catchment_polygon.area)
