import argparse
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from config import OUTPUT_DIR, HEC_INPUT_DSS, HEC_OUTPUT_DSS, FILE_REMOVE_CMD, STATE_INTERVAL
from input.gage.model_gage import create_gage_file_by_rain_file
from input.control.model_control import create_control_file_by_rain_file
from input.run.model_run import create_run_file
from model.model_execute import execute_pre_dssvue, execute_post_dssvue, execute_hechms
from uploads.upload_discharge import extract_distrubuted_hechms_outputs
from input.rainfall.mean_rain import get_mean_rain


HEC_HMS_MODEL_DIR = os.path.join(OUTPUT_DIR, 'distributed_model')
HEC_HMS_STATE_DIR = os.path.join(OUTPUT_DIR, 'distributed_model', 'basinStates')
COPY_MODEL_TEMPLATE_CMD = 'yes | cp -R /home/curw/git/distributed_hechms/distributed_model_template/* /home/curw/git/distributed_hechms/output/distributed_model'
COPY_MODEL_TEMPLATE1_CMD = 'yes | cp -R /home/curw/git/distributed_hechms/distributed_model_template1/* /home/curw/git/distributed_hechms/output/distributed_model'
STATE_BACKUP_DIR = '/home/curw/basin_states'
COPY_STATE_FILES_CMD = 'yes | cp -R /home/curw/basin_states/* /home/curw/git/distributed_hechms/output/distributed_model/basinStates'

FILE_COPY_CMD_TEMPLATE = 'yes | cp -R {} {}'


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_state_file_name(ts_start_datetime):
    startDateTime = datetime.strptime(ts_start_datetime, '%Y-%m-%d %H:%M:%S')
    saveStateDateTime = startDateTime + timedelta(minutes=STATE_INTERVAL)
    # startStateDateTime = startDateTime - timedelta(minutes=STATE_INTERVAL)
    state_file_name = 'State_{}_To_{}.state'.format(startDateTime.strftime('%Y_%m_%d'),
                                                    saveStateDateTime.strftime('%Y_%m_%d'))
    state_file = os.path.join(HEC_HMS_STATE_DIR, state_file_name)
    print('get_state_file_name|state_file : ', state_file)
    return state_file


def run_hechms_workflow(db_user, db_pwd, db_host, db_name, run_datetime=datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), back_days=2, forward_days=3,
                        initial_wl=0, pop_method='MME', target_model='hechms_prod'):
    print('prepare_input_files.')
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
        get_mean_rain(from_date, to_date, output_dir, 'hechms', pop_method, db_user, db_pwd, db_host, db_name)
        rain_fall_file = Path(output_file)
        if rain_fall_file.is_file():
            create_dir_if_not_exists(os.path.join(OUTPUT_DIR, 'distributed_model'))
            if target_model == 'hechms_prod' :
                subprocess.call(COPY_MODEL_TEMPLATE_CMD, shell=True)
            else:
                subprocess.call(COPY_MODEL_TEMPLATE1_CMD, shell=True)
            subprocess.call(COPY_STATE_FILES_CMD, shell=True)
            create_gage_file_by_rain_file('distributed_model', output_file)
            create_control_file_by_rain_file('distributed_model', output_file)
            create_run_file('distributed_model', initial_wl, run_datetime.strftime('%Y-%m-%d %H:%M:%S'), from_date)
            state_file = get_state_file_name(run_datetime.strftime('%Y-%m-%d %H:%M:%S'))
            hechms_input = os.path.join(HEC_HMS_MODEL_DIR, HEC_INPUT_DSS.replace('{MODEL_NAME}', 'distributed_model'))
            hechms_output = os.path.join(HEC_HMS_MODEL_DIR, HEC_OUTPUT_DSS.replace('{MODEL_NAME}', 'distributed_model'))
            try:
                print('hechms_input : ', hechms_input)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_input), shell=True)
                print('hechms_output : ', hechms_output)
                subprocess.call(FILE_REMOVE_CMD.replace('{FILE_NAME}', hechms_output), shell=True)
                ts_start_date = (datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                print('[ts_start_date, ts_start_time] : ', [ts_start_date, ts_start_time])
                ret_code = execute_pre_dssvue(exec_datetime, ts_start_date, ts_start_time)
                print('execute_pre_dssvue|ret_code : ', ret_code)
                if ret_code == 0:
                    ret_code = execute_hechms('distributed_model', HEC_HMS_MODEL_DIR)
                    print('execute_hechms|ret_code : ', ret_code)
                    if ret_code == 0:
                        ret_code = execute_post_dssvue(exec_datetime, ts_start_date, ts_start_time)
                        print('execute_post_dssvue|ret_code : ', ret_code)
                        if ret_code == 0:
                            output_dir = os.path.join(OUTPUT_DIR, file_date, file_time)
                            print('output_dir : ', output_dir)
                            output_file = os.path.join(output_dir, 'DailyDischarge.csv')
                            state_file_copy_cmd = FILE_COPY_CMD_TEMPLATE.format(state_file, STATE_BACKUP_DIR)
                            print('state_file_copy_cmd : ', state_file_copy_cmd)
                            subprocess.call(state_file_copy_cmd, shell=True)
                            try:
                                print('extract_distrubuted_hechms_outputs|[output_file, file_date] : ',
                                      [output_file, file_date])
                                extract_distrubuted_hechms_outputs(target_model, db_user, db_pwd, db_host, 'curw_fcst', output_file, file_date, '00:00:00')
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

