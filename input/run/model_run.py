from config import RUN_FILE_TEMPLATE0, RUN_FILE_TEMPLATE1, RUN_FILE_NAME, STATE_INTERVAL
from datetime import datetime, timedelta
import collections

DSSDateTime = collections.namedtuple('DSSDateTime', ['dateTime', 'date', 'time'])


def create_run_file(model_name, initial_wl, date_time, ts_start_datetime):
    startDateTime = datetime.strptime(ts_start_datetime, '%Y-%m-%d %H:%M:%S')
    saveStateDateTime = startDateTime + timedelta(minutes=STATE_INTERVAL)
    startStateDateTime = startDateTime - timedelta(minutes=STATE_INTERVAL)
    if initial_wl == 0:
        create_run_file0(model_name, date_time, startDateTime, saveStateDateTime)
    else:
        create_run_file1(model_name, date_time, startDateTime, saveStateDateTime, startStateDateTime)


def get_dss_date_time(date_time):
    # Removed DSS formatting with HEC-HMS upgrading from 3.5 to 4.1
    my_date = date_time.strftime('%d %B %Y')
    my_time = date_time.strftime('%H:%M')

    return DSSDateTime(
        dateTime=my_date + ' ' + my_time,
        date=my_date,
        time=my_time
    )


def create_run_file0(model_name, date_time, startDateTime, saveStateDateTime):
    run_file_path = RUN_FILE_NAME.replace('{MODEL_NAME}', model_name)
    run_file = RUN_FILE_TEMPLATE0.replace('{MODEL_NAME}', model_name)
    current_date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
    last_modified_date = current_date_time.strftime('%d %b %Y')
    last_modified_time = current_date_time.strftime('%H:%M:%S')

    run_file = run_file.replace('{LAST_MODIFIED_DATE}', last_modified_date)
    run_file = run_file.replace('{LAST_MODIFIED_TIME}', last_modified_time)

    date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

    execution_date = date_time.strftime('%d %b %Y')
    execution_time = date_time.strftime('%H:%M:%S')

    run_file = run_file.replace('{EXECUSION_DATE}', execution_date)
    run_file = run_file.replace('{EXECUSION_TIME}', execution_time)

    saveStateDateTimeDSS = get_dss_date_time(saveStateDateTime)
    run_file = run_file.replace('{START_STATE_DATE}', startDateTime.strftime('%Y_%m_%d'))
    run_file = run_file.replace('{SAVE_STATE_DATE_TIME}', saveStateDateTime.strftime('%Y_%m_%d'))

    run_file = run_file.replace('{SAVE_STATE_DATE_DSS}', saveStateDateTimeDSS.date)
    run_file = run_file.replace('{SAVE_STATE_TIME_DSS}', saveStateDateTimeDSS.time)

    with open(run_file_path, 'w') as file:
        file.write(run_file)
        file.write('\n\n')
    file.close()


def create_run_file1(model_name, date_time, startDateTime, saveStateDateTime, startStateDateTime):
    """
    :param model_name: str 'model_name'
    :param date_time: str '2019-09-01 00:00:00'
    :return:
    """
    run_file_path = RUN_FILE_NAME.replace('{MODEL_NAME}', model_name)
    run_file = RUN_FILE_TEMPLATE1.replace('{MODEL_NAME}', model_name)
    current_date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
    last_modified_date = current_date_time.strftime('%d %b %Y')
    last_modified_time = current_date_time.strftime('%H:%M:%S')

    run_file = run_file.replace('{LAST_MODIFIED_DATE}', last_modified_date)
    run_file = run_file.replace('{LAST_MODIFIED_TIME}', last_modified_time)

    date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

    execution_date = date_time.strftime('%d %b %Y')
    execution_time = date_time.strftime('%H:%M:%S')

    run_file = run_file.replace('{EXECUSION_DATE}', execution_date)
    run_file = run_file.replace('{EXECUSION_TIME}', execution_time)

    saveStateDateTimeDSS = get_dss_date_time(saveStateDateTime)
    run_file = run_file.replace('{START_STATE_DATE}', startDateTime.strftime('%Y_%m_%d'))
    run_file = run_file.replace('{SAVE_STATE_DATE_TIME}', saveStateDateTime.strftime('%Y_%m_%d'))

    run_file = run_file.replace('{SAVE_STATE_DATE_DSS}', saveStateDateTimeDSS.date)
    run_file = run_file.replace('{SAVE_STATE_TIME_DSS}', saveStateDateTimeDSS.time)

    run_file = run_file.replace('{START_STATE_DATE_TIME}', startStateDateTime.strftime('%Y_%m_%d'))
    run_file = run_file.replace('{START_STATE_DATE}', startDateTime.strftime('%Y_%m_%d'))

    with open(run_file_path, 'w') as file:
        file.write(run_file)
        file.write('\n\n')
    file.close()

