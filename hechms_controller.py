import argparse
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import geopandas as gpd
from config import HEC_INPUT_DSS, HEC_OUTPUT_DSS, FILE_REMOVE_CMD, STATE_INTERVAL, RESOURCE_PATH
from input.gage.model_gage import create_gage_file_by_rain_file
from input.control.model_control import create_control_file_by_rain_file
from input.run.model_run import create_run_file
from model.model_execute import execute_pre_dssvue, execute_post_dssvue, execute_hechms
from uploads.upload_discharge import extract_distrubuted_hechms_outputs
from input.rainfall.mean_rain import get_mean_rain, get_basin_init_discharge
from input.rainfall.event_rain import get_basin_rain, get_basin_init_discharge
from decimal import Decimal
from google.cloud import storage

###--------------------------------------------------------------------------------------------------------------------------------
GOOGLE_BUCKET_KEY_PATH = '/home/curw/uwcc-admin.json'
BUCKET_NAME = 'curwsl_nfs'

OUTPUT_DIR = '/home/curw/git/distributed_hechms/output'
HEC_HMS_MODEL_DIR = os.path.join(OUTPUT_DIR, 'hechms_model')
HEC_HMS_STATE_DIR = os.path.join(OUTPUT_DIR, 'hechms_model', 'basinStates')

COPY_TEMPLATE_CMD = 'yes | cp -R /home/curw/git/distributed_hechms/templates/{}/* /home/curw/git/distributed_hechms/output/hechms_model'

STATE_BACKUP_DIR = '/home/curw/basin_states'
COPY_STATE_FILES_CMD = 'yes | cp -R /home/curw/basin_states/* /home/curw/git/distributed_hechms/output/hechms_model/basinStates'
##----------------------------------------------------------------------------------------------------------------------------------


FILE_COPY_CMD_TEMPLATE = 'yes | cp -R {} {}'
ALLOWED_RAIN_ERROR = 0.25


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_state_file_name(ts_start_datetime):
    print('get_state_file_name|ts_start_datetime : ', ts_start_datetime)
    startDateTime = datetime.strptime(ts_start_datetime, '%Y-%m-%d %H:%M:%S')
    saveStateDateTime = startDateTime + timedelta(minutes=STATE_INTERVAL)
    # startStateDateTime = startDateTime - timedelta(minutes=STATE_INTERVAL)
    state_file_name = 'State_{}_To_{}.state'.format(startDateTime.strftime('%Y_%m_%d'),
                                                    saveStateDateTime.strftime('%Y_%m_%d'))
    state_file = os.path.join(HEC_HMS_STATE_DIR, state_file_name)
    print('get_state_file_name|state_file : ', state_file)
    return state_file


def run_hechms_workflow(db_user, db_pwd, db_host, db_name, run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                        back_days=2, forward_days=3, initial_wl=0, pop_method='MME', target_model='HDC'):
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)
    print('initial_wl : ', initial_wl)
    print('pop_method : ', pop_method)
    exec_datetime = datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')
    # exec_datetime = exec_datetime.strftime('%Y-%m-%d %H:%M:%S')
    file_date = (datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')).strftime('%Y-%m-%d')
    print('file_date : ', file_date)
    file_time = (datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')).strftime('%H:%M:%S')
    print('file_time : ', file_time)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')
    run_datetime = datetime.strptime(run_datetime.strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
    print('run_datetime : ', run_datetime)
    to_date = run_datetime + timedelta(days=forward_days)
    from_date = run_datetime - timedelta(days=back_days)
    from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
    print('{from_date, to_date} : ', {from_date, to_date})
    # output_dir = os.path.join(OUTPUT_DIR, file_date, file_time)
    output_dir = OUTPUT_DIR
    print('output_dir : ', output_dir)
    output_file = os.path.join(output_dir, 'DailyRain.csv')
    try:
        create_dir_if_not_exists(output_dir)
        print('run_hechms_workflow|get_basin_rain|start')
        get_basin_rain(from_date, to_date, output_dir, 'hechms', pop_method, ALLOWED_RAIN_ERROR, exec_datetime.strftime(
            '%Y-%m-%d %H:00:00'), db_user, db_pwd, db_host, db_name, target_model)
        print('run_hechms_workflow|get_basin_rain|end')
        rain_fall_file = Path(output_file)
        if rain_fall_file.is_file():
            create_dir_if_not_exists(os.path.join(OUTPUT_DIR, 'hechms_model'))
            subprocess.call(COPY_TEMPLATE_CMD.format(target_model), shell=True)
            create_gage_file_by_rain_file('hechms_model', output_file)
            create_control_file_by_rain_file('hechms_model', output_file)
            create_run_file('hechms_model', initial_wl, run_datetime.strftime('%Y-%m-%d %H:%M:%S'), from_date)
            state_file = get_state_file_name(from_date)
            hechms_input = os.path.join(HEC_HMS_MODEL_DIR, HEC_INPUT_DSS.replace('{MODEL_NAME}', 'hechms_model'))
            hechms_output = os.path.join(HEC_HMS_MODEL_DIR, HEC_OUTPUT_DSS.replace('{MODEL_NAME}', 'hechms_model'))
            try:
                print('hechms_input : ', hechms_input)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_input), shell=True)
                print('hechms_output : ', hechms_output)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_output), shell=True)
                ts_start_date = (datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                print('[ts_start_date, ts_start_time] : ', [ts_start_date, ts_start_time])
                sub_catchment_shape_file = os.path.join(RESOURCE_PATH, 'sub_catchments/sub_subcatchments.shp')
                update_basin_init_values('{} {}'.format(ts_start_date, ts_start_time), db_user, db_pwd, db_host,
                                         sub_catchment_shape_file, target_model)
                ret_code = execute_pre_dssvue(exec_datetime, ts_start_date, ts_start_time)
                print('execute_pre_dssvue|ret_code : ', ret_code)
                if ret_code == 0:
                    ret_code = execute_hechms('hechms_model', HEC_HMS_MODEL_DIR)
                    print('execute_hechms|ret_code : ', ret_code)
                    if ret_code == 0:
                        ret_code = execute_post_dssvue(exec_datetime, ts_start_date, ts_start_time)
                        print('execute_post_dssvue|ret_code : ', ret_code)
                        if ret_code == 0:
                            output_file = os.path.join(OUTPUT_DIR, 'DailyDischarge.csv')
                            print('output_file : ', output_file)
                            # state_file_copy_cmd = FILE_COPY_CMD_TEMPLATE.format(state_file, STATE_BACKUP_DIR)
                            # print('state_file_copy_cmd : ', state_file_copy_cmd)
                            # subprocess.call(state_file_copy_cmd, shell=True)
                            try:
                                # print('extract_distrubuted_hechms_outputs|[output_file, file_date] : ',
                                #       [output_file, file_date])
                                # print('extract_distrubuted_hechms_outputs|cloud|started')
                                # extract_distrubuted_hechms_outputs(target_model, db_user, db_pwd, db_host, 'curw_fcst',
                                #                                    output_file, file_date, '00:00:00')
                                # print('extract_distrubuted_hechms_outputs|cloud|completed')

                                upload_rain_file = os.path.join(OUTPUT_DIR, 'DailyRain.csv')
                                upload_discharge_file = os.path.join(OUTPUT_DIR, 'DailyDischarge.csv')
                                bucket_rain_file = 'hechms/{}/{}/{}/DailyRain.csv'.format(target_model, file_date,
                                                                                             file_time)
                                bucket_discharge_file = 'hechms/{}/{}/{}/DailyDischarge.csv'.format(target_model, file_date,
                                                                                                       file_time)
                                print('extract_distrubuted_hechms_outputs|upload_rain_file : ', upload_rain_file)
                                print('extract_distrubuted_hechms_outputs|bucket_rain_file : ', bucket_rain_file)
                                print('extract_distrubuted_hechms_outputs|upload_discharge_file : ',
                                      upload_discharge_file)
                                print('extract_distrubuted_hechms_outputs|bucket_discharge_file : ',
                                      bucket_discharge_file)
                                print('extract_distrubuted_hechms_outputs|file upload started')
                                try:
                                    upload_file_to_bucket(GOOGLE_BUCKET_KEY_PATH, BUCKET_NAME, upload_rain_file,
                                                          bucket_rain_file)
                                    upload_file_to_bucket(GOOGLE_BUCKET_KEY_PATH, BUCKET_NAME, upload_discharge_file,
                                                          bucket_discharge_file)
                                    print('extract_distrubuted_hechms_outputs|file upload completed')
                                except Exception as e:
                                    print('extract_distrubuted_hechms_outputs|Exception : ', str(e))
                                return True
                            except Exception as e:
                                return False
                        else:
                            return False
                    else:
                        return False
                else:
                    print('pre-processing has failed')
                    return False
            except Exception as e:
                print('Remove hechms input/output files|Exception: ', e)
                return False
        else:
            print('input mean rain file creation has failed')
            return False
    except Exception as e:
        print('prepare_input_files|Exception: ', e)
        return False


def update_basin_init_values(init_date_time, db_user, db_pwd, db_host, sub_catchment_shape_file, target_model):
    print('update_basin_init_values|init_date_time : ', init_date_time)
    init_discharge = get_basin_init_discharge(init_date_time, db_user, db_pwd, db_host)
    print('update_basin_init_values|init_discharge : ', init_discharge)
    if init_discharge is not None:
        basin_template_file = os.path.join(OUTPUT_DIR, 'hechms_model', 'hechms_model_template.basin')
        basin_file = os.path.join(OUTPUT_DIR, 'hechms_model', 'hechms_model.basin')
        if target_model == 'HDC' or target_model == 'HDE':
            area_ratio = get_sub_catchment_area_ratios(sub_catchment_shape_file)
            print('update_basin_init_values|area_ratio : ', area_ratio)
            template = open(basin_template_file, 'r')
            lines = template.readlines()
            line_count = 1
            with open(basin_file, 'w') as actual:
                for line in lines:
                    if target_model == 'HDC':
                        if line_count == 62:
                            discharge_value1 = init_discharge * area_ratio['SB-1']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value1)
                        elif line_count == 129:
                            discharge_value2 = init_discharge * area_ratio['SB-3']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value2)
                        elif line_count == 179:
                            discharge_value3 = init_discharge * area_ratio['SB-2']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value3)
                        elif line_count == 255:
                            discharge_value4 = init_discharge * area_ratio['SB-4']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value4)
                        elif line_count == 304:
                            discharge_value5 = init_discharge * area_ratio['SB-5']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value5)
                        else:
                            new_line = line
                        actual.write(new_line)
                        line_count += 1
                    elif target_model == 'HDE':
                        if line_count == 45:
                            discharge_value1 = init_discharge * area_ratio['SB-1']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value1)
                        elif line_count == 95:
                            discharge_value2 = init_discharge * area_ratio['SB-3']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value2)
                        elif line_count == 128:
                            discharge_value3 = init_discharge * area_ratio['SB-2']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value3)
                        elif line_count == 187:
                            discharge_value4 = init_discharge * area_ratio['SB-4']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value4)
                        elif line_count == 219:
                            discharge_value5 = init_discharge * area_ratio['SB-5']
                            new_line = '     Initial Baseflow: {}\n'.format(discharge_value5)
                        else:
                            new_line = line
                        actual.write(new_line)
                        line_count += 1
            template.close()
        else:
            template = open(basin_template_file, 'r')
            lines = template.readlines()
            line_count = 1
            with open(basin_file, 'w') as actual:
                for line in lines:
                    if target_model == 'HLC':
                        if line_count == 61:
                            new_line = '     Initial Baseflow: {}\n'.format(init_discharge)
                        else:
                            new_line = line
                        actual.write(new_line)
                        line_count += 1
                    elif target_model == 'HLE':
                        if line_count == 44:
                            new_line = '     Initial Baseflow: {}\n'.format(init_discharge)
                        else:
                            new_line = line
                        actual.write(new_line)
                        line_count += 1
            template.close()


def get_sub_catchment_area_ratios(sub_catchment_shape_file):
    catchment_df = gpd.GeoDataFrame.from_file(sub_catchment_shape_file)
    # print('get_sub_catchment_area_ratios|catchment_df : ', catchment_df)
    total_area = 0
    for index, row in catchment_df.iterrows():
        total_area += row['Area']
    # print('get_sub_catchment_area_ratios|total_area : ', total_area)
    area_ratio = {}
    for index, row in catchment_df.iterrows():
        area_ratio[row['Name_of_Su']] = Decimal(row['Area'] / total_area)
    # print('get_sub_catchment_area_ratios|area_ratio : ', area_ratio)
    return area_ratio


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-run_datetime')
    parser.add_argument('-forward', default=3)
    parser.add_argument('-backward', default=2)
    parser.add_argument('-init_run', default=0)
    parser.add_argument('-pop_method', default='MME')
    parser.add_argument('-db_user')
    parser.add_argument('-db_pwd')
    parser.add_argument('-db_host')
    parser.add_argument('-db_name')
    parser.add_argument('-target_model')
    return parser.parse_args()


def upload_file_to_bucket(key_file, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    client = storage.Client.from_service_account_json(key_file)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


if __name__ == '__main__':
    args = vars(parse_args())
    print('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))
    run_datetime = args['run_datetime']
    forward = int(args['forward'])
    backward = int(args['backward'])
    init_run = int(args['init_run'])
    pop_method = args['pop_method']
    db_user = args['db_user']
    db_pwd = args['db_pwd']
    db_host = args['db_host']
    db_name = args['db_name']
    target_model = args['target_model']
    print('**** HECHMS RUN **** run_datetime: {}'.format(run_datetime))
    print('**** HECHMS RUN **** forward: {}'.format(forward))
    print('**** HECHMS RUN **** backward: {}'.format(backward))
    print('**** HECHMS RUN **** init_run: {}'.format(init_run))
    print('**** HECHMS RUN **** pop_method: {}'.format(pop_method))
    print('**** HECHMS RUN **** target_model: {}'.format(target_model))
    if run_hechms_workflow(db_user, db_pwd, db_host, db_name, run_datetime, backward, forward, init_run, pop_method,
                           target_model):
        print('**** HECHMS RUN Completed****')
    else:
        print('**** HECHMS RUN Failed****')


# if __name__ == '__main__':
#     db_user = 'fcst_pusher'
#     db_pwd = 'aquafcst'
#     db_host = '35.197.98.125'
#     db_name = 'curw_sim'
#     run_datetime = '2020-06-17_14:00:00'
#     backward = '3'
#     forward = '2'
#     init_run = '0'
#     pop_method = 'MME'
#     target_model = 'HDC'
#     if run_hechms_workflow(db_user, db_pwd, db_host, db_name, run_datetime, backward, forward, init_run, pop_method,
#                            target_model):
#         print('**** HECHMS RUN Completed****')
#     else:
#         print('**** HECHMS RUN Failed****')
