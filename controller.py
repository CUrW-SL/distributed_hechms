import os
import subprocess
from flask import Flask, request, jsonify
from flask_json import FlaskJSON, JsonError, json_response
from os import path
from datetime import datetime, timedelta
from pathlib import Path

from config import UPLOADS_DEFAULT_DEST, INIT_DATE_TIME_FORMAT,\
    OUTPUT_DIR, HEC_INPUT_DSS, HEC_OUTPUT_DSS, FILE_REMOVE_CMD
from input.shape_util.polygon_util import get_rain_files
from input.gage.model_gage import create_gage_file_by_rain_file
from input.control.model_control import create_control_file, create_control_file_by_rain_file
from input.run.model_run import create_run_file
from model.model_execute import execute_pre_dssvue, execute_post_dssvue, execute_hechms
from uploads.upload_discharge import extract_distrubuted_hechms_outputs

from input.rainfall.mean_rain import get_mean_rain
from input.event_rain.create_rainfall import get_event_mean_rain


import logging
logging.basicConfig(filename="home/curw/git/distributed_hechms/output/hechms.log", level=logging.DEBUG)

HEC_HMS_MODEL_DIR = os.path.join(OUTPUT_DIR, 'distributed_model')
COPY_MODEL_TEMPLATE_CMD = 'cp -R /home/curw/git/distributed_hechms/distributed_model_template/* /home/curw/git/distributed_hechms/output/distributed_model'

app = Flask(__name__)
flask_json = FlaskJSON()

# Flask-Uploads configs
app.config['UPLOADS_DEFAULT_DEST'] = path.join(UPLOADS_DEFAULT_DEST, 'FLO2D')
app.config['UPLOADED_FILES_ALLOW'] = ['csv', 'run', 'control']

# upload set creation
flask_json.init_app(app)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@app.route('/')
def hello_world():
    return 'Welcome to HecHms(Distributed) Server!'


@app.route('/HECHMS/distributed/init', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/init/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/init/<string:run_datetime>/<int:back_days>/<int:forward_days>/<int:initial_wl>/<string:pop_method>',  methods=['GET', 'POST'])
def prepare_input_files(run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), back_days=2, forward_days=3,
                        initial_wl=0, pop_method='MME'):
    print('prepare_input_files.')
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)
    print('initial_wl : ', initial_wl)
    print('pop_method : ', pop_method)
    exec_datetime = run_datetime
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
    output_dir = os.path.join(OUTPUT_DIR, file_date, file_time)
    print('output_dir : ', output_dir)
    output_file = os.path.join(output_dir, 'DailyRain.csv')
    try:
        create_dir_if_not_exists(output_dir)
        if pop_method.isupper():
            print('Capital tag has used|pop_method : ', pop_method)
            get_mean_rain(from_date, to_date, output_dir, 'hechms', pop_method)
        else:
            print('Event tag has used|pop_method : ', pop_method)
            sim_tag = pop_method[:len(pop_method) - 3]
            wrf_model = int(pop_method[len(pop_method) - 2:])
            print('prepare_input_files|[sim_tag, wrf_model, exec_datetime] : ', [sim_tag, wrf_model, exec_datetime])
            get_event_mean_rain(exec_datetime, forward_days, back_days, output_dir, wrf_model, sim_tag)
        rain_fall_file = Path(output_file)
        if rain_fall_file.is_file():
            create_dir_if_not_exists(os.path.join(OUTPUT_DIR, 'distributed_model'))
            subprocess.call(COPY_MODEL_TEMPLATE_CMD, shell=True)
            create_gage_file_by_rain_file('distributed_model', output_file)
            create_control_file_by_rain_file('distributed_model', output_file)
            create_run_file('distributed_model', initial_wl, run_datetime.strftime('%Y-%m-%d %H:%M:%S'), from_date)
            hechms_input = os.path.join(HEC_HMS_MODEL_DIR, HEC_INPUT_DSS.replace('{MODEL_NAME}', 'distributed_model'))
            hechms_output = os.path.join(HEC_HMS_MODEL_DIR, HEC_OUTPUT_DSS.replace('{MODEL_NAME}', 'distributed_model'))
            try:
                print('hechms_input : ', hechms_input)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_input), shell=True)
                print('hechms_output : ', hechms_output)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_output), shell=True)
            except Exception as e:
                print('Remove hechms input/output files|Exception: ', e)
                logging.debug("Remove hechms input/output files|Exception|{}".format(e))
            return jsonify({'Result': 'Success'})
        else:
            return jsonify({'Result': 'Fail'})
    except Exception as e:
        print('prepare_input_files|Exception: ', e)
        logging.debug("prepare_input_files|Exception|{}".format(e))
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/pre-process/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def pre_processing(run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), back_days=3, forward_days=2):
    print('pre_processing.')
    print('run_datetime : ', run_datetime)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')
    exec_datetime = run_datetime
    run_datetime = datetime.strptime(run_datetime.strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
    from_date = run_datetime - timedelta(days=back_days)
    ts_start_date = from_date.strftime('%Y-%m-%d')
    ts_start_time = from_date.strftime('%H:%M:%S')
    print('[ts_start_date, ts_start_time] : ', [ts_start_date, ts_start_time])
    # ts_end = to_date.strftime('%Y-%m-%d %H:%M:%S')
    ret_code = execute_pre_dssvue(exec_datetime, ts_start_date, ts_start_time)
    if ret_code == 0:
        return jsonify({'Result': 'Success'})
    else:
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/run', methods=['GET', 'POST'])
def run_hec_hms_model():
    print('run_hec_hms_model.')
    ret_code = execute_hechms('distributed_model', HEC_HMS_MODEL_DIR)
    if ret_code == 0:
        return jsonify({'Result': 'Success'})
    else:
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/post-process/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def post_processing(run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), back_days=3, forward_days=2):
    print('pre_processing.')
    print('run_datetime : ', run_datetime)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')
    exec_datetime = run_datetime
    run_datetime = datetime.strptime(run_datetime.strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
    from_date = run_datetime - timedelta(days=back_days)
    ts_start_date = from_date.strftime('%Y-%m-%d')
    ts_start_time = from_date.strftime('%H:%M:%S')
    ret_code = execute_post_dssvue(exec_datetime, ts_start_date, ts_start_time)
    if ret_code == 0:
        return jsonify({'Result': 'Success'})
    else:
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/upload-discharge/<string:run_datetime>/<string:model>', methods=['GET', 'POST'])
def upload_discharge(run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), model='distributed'):
    print('upload_discharge..')
    print('run_datetime : ', run_datetime)
    print('model : ', model)
    file_date = (datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')).strftime('%Y-%m-%d')
    print('file_date : ', file_date)
    file_time = (datetime.strptime(run_datetime, '%Y-%m-%d_%H:%M:%S')).strftime('%H:%M:%S')
    print('file_time : ', file_time)
    output_dir = os.path.join(OUTPUT_DIR, file_date, file_time)
    print('output_dir : ', output_dir)
    output_file = os.path.join(output_dir, 'DailyDischarge.csv')
    try:
        print('extract_distrubuted_hechms_outputs|[output_file, file_date] : ', [output_file, file_date])
        response = extract_distrubuted_hechms_outputs(output_file, file_date, '00:00:00')
        return jsonify(response)
    except Exception as e:
        return jsonify({'Result': 'Fail'})


# @app.route('/HECHMS/distributed/rain-fall', methods=['GET', 'POST'])
# @app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>',  methods=['GET', 'POST'])
# @app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
# def get_sub_catchment_rain_fall(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
#     print('get_sub_catchment_rain_fall.')
#     print('run_datetime : ', run_datetime)
#     print('back_days : ', back_days)
#     print('forward_days : ', forward_days)
#
#     run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
#     to_date = run_datetime + timedelta(days=forward_days)
#     from_date = run_datetime - timedelta(days=back_days)
#     file_date = run_datetime.strftime('%Y-%m-%d')
#     from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
#     to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
#     file_name = RAIN_FALL_FILE_NAME.format(file_date)
#     print('file_name : ', file_name)
#     print('{from_date, to_date} : ', {from_date, to_date})
#     # get_sub_catchment_rain_files(file_name, from_date, to_date)
#     get_rain_files(file_name, run_datetime.strftime('%Y-%m-%d %H:%M:%S'), forward_days, back_days)
#     return jsonify({'timeseries': {}})


# @app.route('/HECHMS/distributed/create-gage-file', methods=['GET', 'POST'])
# @app.route('/HECHMS/distributed/create-gage-file/<string:run_datetime>',  methods=['GET', 'POST'])
# @app.route('/HECHMS/distributed/create-gage-file/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
# def get_gage_file(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
#     print('create_gage_file.')
#     print('run_datetime : ', run_datetime)
#     print('back_days : ', back_days)
#     print('forward_days : ', forward_days)
#     run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
#     to_date = run_datetime + timedelta(days=forward_days)
#     from_date = run_datetime - timedelta(days=back_days)
#     file_date = run_datetime.strftime('%Y-%m-%d')
#     from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
#     to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
#     file_name = RAIN_FALL_FILE_NAME.format(file_date)
#     rain_fall_file = Path(file_name)
#     if rain_fall_file.is_file():
#         create_gage_file_by_rain_file('distributed_model', file_name)
#     return jsonify({'timeseries': {}})


@app.route('/HECHMS/distributed/create-control-file', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/create-control-file/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/create-control-file/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def get_control_file(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
    print('create_gage_file.')
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    to_date = run_datetime + timedelta(days=forward_days)
    from_date = run_datetime - timedelta(days=back_days)
    file_date = run_datetime.strftime('%Y-%m-%d')
    from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
    file_name = 'output/DailyRain-{}.csv'.format(file_date)
    rain_fall_file = Path(file_name)
    if rain_fall_file.is_file():
        create_control_file_by_rain_file('distributed_model', file_name)
    else:
        create_control_file('distributed_model', from_date, to_date)
    return jsonify({'timeseries': {}})


@app.route('/HECHMS/distributed/create-run-file', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/create-run-file/<string:run_datetime>',  methods=['GET', 'POST'])
def get_run_file(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S')):
    print('create_run_file.')
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    create_run_file('distributed_model', run_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    return jsonify({'timeseries': {}})


def is_valid_run_name(run_name):
    """
    Checks the validity of the run_name. run_name cannot have spaces or colons.
    :param run_name: <class str> provided run_name.
    :return: <bool> True if valid False if not.
    """
    return run_name and not (' ' in run_name or ':' in run_name)


def is_valid_init_dt(date_time):
    """
    Checks the validity of given date_time. Given date_time should be of "yyyy-mm-dd_HH:MM:SS"
    :param date_time: datetime instance
    :return: boolean, True if valid False otherwise
    """
    try:
        datetime.strptime(date_time, INIT_DATE_TIME_FORMAT)
        return True
    except ValueError:
        return False


if __name__ == '__main__':
    app.run(host='192.168.1.43', port=5000)
    #app.run(port=5000)

# /home/curw/distributed_hec/hechms-distributed
