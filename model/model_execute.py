import subprocess
from datetime import timedelta
from os import path

from config import HEC_HMS_HOME, HEC_HMS_SH, HEC_DSSVUE_HOME, HEC_DSSVUE_SH, HEC_EVENT_SCRIPT,\
    PRE_PROCESSING_SCRIPT,POST_PROCESSING_SCRIPT, RAIN_FALL_FILE_NAME, DISCHARGE_FILE_NAME, \
    HEC_INPUT_DSS, HEC_OUTPUT_DSS


def execute_pre_dssvue(run_date_time, back_days):
    python_script_fp = PRE_PROCESSING_SCRIPT
    run_date = run_date_time.strftime('%Y-%m-%d')
    run_time = run_date_time.strftime('%H:%M:%S')
    ts_start_date_time = run_date_time - timedelta(days=back_days)
    ts_start_date = ts_start_date_time.strftime('%Y-%m-%d')
    ts_start_time = ts_start_date_time.strftime('%H:%M:%S')
    return _execute_hec_dssvue(python_script_fp, run_date, run_time, ts_start_date, ts_start_time)


def execute_post_dssvue(run_date_time, back_days):
    python_script_fp = POST_PROCESSING_SCRIPT
    run_date = run_date_time.strftime('%Y-%m-%d')
    run_time = run_date_time.strftime('%H:%M:%S')
    ts_start_date_time = run_date_time - timedelta(days=back_days)
    ts_start_date = ts_start_date_time.strftime('%Y-%m-%d')
    ts_start_time = ts_start_date_time.strftime('%H:%M:%S')
    return _execute_hec_dssvue(python_script_fp, run_date, run_time, ts_start_date, ts_start_time)


def _execute_hec_dssvue(python_script, run_date, run_time, ts_start_date, ts_start_time):
    dssvue_sh = path.join(HEC_DSSVUE_HOME, HEC_DSSVUE_SH)
    #bash_command = '/home/curw/distributed_hec/hec-dssvue201/hec-dssvue.sh {PYTHON_SCRIPT} --date 2019-02-20 --time 14:00:00 --start-date 2019-02-18 --start-time 14:00:00'
    bash_command = '{dssvue_sh} {python_script} --date {run_date} --time {run_time} --start-date {ts_start_date} --start-time {ts_start_time}'\
        .format(dssvue_sh=dssvue_sh, python_script=python_script, run_date=run_date, run_time=run_time, ts_start_date=ts_start_date, ts_start_time=ts_start_time)
    print('execute_hec_dssvue|bash_command : ', bash_command)
    ret_code = subprocess.call(bash_command, shell=True)
    return ret_code


def execute_hechms(model_name, run_path):
    hec_hms_sh_fp = path.join(HEC_HMS_HOME, HEC_HMS_SH)
    model_event_script_fp = path.join(run_path, HEC_EVENT_SCRIPT.replace('{MODEL_NAME}', model_name))
    bash_command = "{hec_hms_sh} -s {hec_event_script}" \
        .format(hec_hms_sh=hec_hms_sh_fp, hec_event_script=model_event_script_fp)
    print('execute_hechms|bash_command : ', bash_command)
    ret_code = subprocess.call(bash_command, shell=True)
    return ret_code
