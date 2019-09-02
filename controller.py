import os

from flask import Flask, request, jsonify
from flask_json import FlaskJSON, JsonError, json_response
from flask_uploads import UploadSet, configure_uploads
from os import path
from datetime import datetime, timedelta
from pathlib import Path

from config import UPLOADS_DEFAULT_DEST, INIT_DATE_TIME_FORMAT, RAIN_FALL_FILE_NAME, HEC_HMS_MODEL_DIR, OUTPUT_DIR
from input.shape_util.polygon_util import get_rain_files
from input.gage.model_gage import create_gage_file_by_rain_file
from input.control.model_control import create_control_file, create_control_file_by_rain_file
from input.run.model_run import create_run_file
from model.model_execute import execute_pre_dssvue, execute_post_dssvue, execute_hechms

from input.rainfall.mean_rain import get_mean_rain

import logging
logging.basicConfig(filename="/home/curw/distributed_hec/HecHmsDistributed/hechms.log", level=logging.DEBUG)

app = Flask(__name__)
flask_json = FlaskJSON()

# Flask-Uploads configs
app.config['UPLOADS_DEFAULT_DEST'] = path.join(UPLOADS_DEFAULT_DEST, 'FLO2D')
app.config['UPLOADED_FILES_ALLOW'] = ['csv', 'run', 'control']

# upload set creation
model_distributed = UploadSet('configfiles', extensions=('csv','run','control'))

configure_uploads(app, model_distributed)
flask_json.init_app(app)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


@app.route('/')
def hello_world():
    return 'Welcome to HecHms(Distributed) Server!'


@app.route('/HECHMS/distributed/init-run', methods=['POST'])
def init_run():
    req_args = request.args.to_dict()
    # Check whether run-name is specified and valid.
    if 'run-name' not in req_args.keys() or not req_args['run-name']:
        raise JsonError(status_=400, description='run-name is not specified.')
    run_name = req_args['run-name']
    if not is_valid_run_name(run_name):
        raise JsonError(status_=400, description='run-name cannot contain spaces or colons.')
    # Valid base-dt must be specified at the initialization phase
    if 'base-dt' not in req_args.keys() or not req_args['base-dt']:
        raise JsonError(status_=400, description='base-dt is not specified.')
    base_dt = req_args['base-dt']
    if not is_valid_init_dt(base_dt):
        raise JsonError(status_=400, description='Given base-dt is not in the correct format: %s'
                                                 % INIT_DATE_TIME_FORMAT)
    # Valid run-dt must be specified at the initialization phase
    if 'run-dt' not in req_args.keys() or not req_args['run-dt']:
        raise JsonError(status_=400, description='run-dt is not specified.')
    run_dt = req_args['run-dt']
    if not is_valid_init_dt(run_dt):
        raise JsonError(status_=400, description='Given run-dt is not in the correct format: %s'
                                                 % INIT_DATE_TIME_FORMAT)

    today = datetime.today().strftime('%Y-%m-%d')
    input_dir_rel_path = path.join(today, run_name, 'input')
    # Check whether the given run-name is already taken for today.
    input_dir_abs_path = path.join(UPLOADS_DEFAULT_DEST, 'HECHMS', 'distributed', input_dir_rel_path)
    if path.exists(input_dir_abs_path):
        raise JsonError(status_=400, description='run-name: %s is already taken for today: %s.' % (run_name, today))

    req_files = request.files
    if 'inflow' in req_files and 'outflow' in req_files and 'raincell' in req_files:
        model_distributed.save(req_files['rainfall'], folder=input_dir_rel_path, name='daily_rain.csv')
        model_distributed.save(req_files['model_run'], folder=input_dir_rel_path, name='model.run')
        model_distributed.save(req_files['model_control'], folder=input_dir_rel_path, name='model.control')
        model_distributed.save(req_files['model_gage'], folder=input_dir_rel_path, name='model.gage')
    else:
        raise JsonError(status_=400, description='Missing required input files. Required inflow, outflow, raincell.')

    run_id = 'FLO2D:model250m:%s:%s' % (today, run_name)  # TODO save run_id in a DB with the status
    return json_response(status_=200, run_id=run_id, description='Successfully saved files.')


@app.route('/HECHMS/distributed/init', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/init/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/init/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def prepare_input_files(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
    print('prepare_input_files.')
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)
    file_date = (datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')
    file_time = (datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')).strftime('%H:%M:%S')
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d 00:00:00')
    to_date = run_datetime + timedelta(days=forward_days)
    from_date = run_datetime - timedelta(days=back_days)
    from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
    output_dir = os.path.join(OUTPUT_DIR, file_date, file_time)
    print('output_dir : ', output_dir)
    print('{from_date, to_date} : ', {from_date, to_date})
    try:
        create_dir_if_not_exists(output_dir)
        get_rain_files(output_dir, from_date, to_date)
        get_mean_rain(from_date, to_date, output_dir)
        # rain_fall_file = Path(file_name)
        # if rain_fall_file.is_file():
        #     create_gage_file_by_rain_file('distributed_model', file_name)
        #     create_control_file_by_rain_file('distributed_model', file_name)
        # else:
        #     #create_gage_file('distributed_model', from_date, to_date)
        #     create_control_file('distributed_model', from_date, to_date)
        # create_run_file('distributed_model', run_datetime.strftime('%Y-%m-%d %H:%M:%S'))
        return jsonify({'Result': 'Success'})
    except Exception as e:
        print('prepare_input_files|Exception: ', e)
        logging.debug("prepare_input_files|Exception|{}".format(e))
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/pre-process/<string:run_datetime>/<int:back_days>',  methods=['GET', 'POST'])
def pre_processing(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2):
    print('pre_processing.')
    print('run_datetime : ', run_datetime)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    ret_code = execute_pre_dssvue(run_datetime, back_days)
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


@app.route('/HECHMS/distributed/post-process/<string:run_datetime>/<int:back_days>',  methods=['GET', 'POST'])
def post_processing(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2):
    print('pre_processing.')
    print('run_datetime : ', run_datetime)
    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    ret_code = execute_post_dssvue(run_datetime, back_days)
    if ret_code == 0:
        return jsonify({'Result': 'Success'})
    else:
        return jsonify({'Result': 'Fail'})


@app.route('/HECHMS/distributed/rain-fall', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/rain-fall/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def get_sub_catchment_rain_fall(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
    print('get_sub_catchment_rain_fall.')
    print('run_datetime : ', run_datetime)
    print('back_days : ', back_days)
    print('forward_days : ', forward_days)

    run_datetime = datetime.strptime(run_datetime, '%Y-%m-%d %H:%M:%S')
    to_date = run_datetime + timedelta(days=forward_days)
    from_date = run_datetime - timedelta(days=back_days)
    file_date = run_datetime.strftime('%Y-%m-%d')
    from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')
    file_name = RAIN_FALL_FILE_NAME.format(file_date)
    print('file_name : ', file_name)
    print('{from_date, to_date} : ', {from_date, to_date})
    # get_sub_catchment_rain_files(file_name, from_date, to_date)
    get_rain_files(file_name, run_datetime.strftime('%Y-%m-%d %H:%M:%S'), forward_days, back_days)
    return jsonify({'timeseries': {}})


@app.route('/HECHMS/distributed/create-gage-file', methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/create-gage-file/<string:run_datetime>',  methods=['GET', 'POST'])
@app.route('/HECHMS/distributed/create-gage-file/<string:run_datetime>/<int:back_days>/<int:forward_days>',  methods=['GET', 'POST'])
def get_gage_file(run_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), back_days=2, forward_days=3):
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
    file_name = RAIN_FALL_FILE_NAME.format(file_date)
    rain_fall_file = Path(file_name)
    if rain_fall_file.is_file():
        create_gage_file_by_rain_file('distributed_model', file_name)
    else:
        create_gage_file('distributed_model', from_date, to_date)
    return jsonify({'timeseries': {}})


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
    app.run(host='192.168.1.42', port=5000)
    #app.run(port=5000)

# /home/curw/distributed_hec/hechms-distributed
