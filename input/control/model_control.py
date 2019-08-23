from config import CONTROL_TEMPLATE, HEC_HMS_VERSION, CONTROL_FILE_NAME
from datetime import datetime
import pandas as pd


def create_control_file_by_rain_file(model_name, rain_filename):
    rf_data_frame = pd.read_csv(rain_filename, sep=',')
    start_datetime = rf_data_frame.iloc[3][0]
    end_datetime = rf_data_frame.iloc[-1][0]
    print('start_datetime : ', start_datetime)
    print('end_datetime : ', end_datetime)

    control_file_path = CONTROL_FILE_NAME.replace('{MODEL_NAME}', model_name)
    control_file = CONTROL_TEMPLATE.replace('{MODEL_NAME}', model_name)
    control_file = control_file.replace('{HEC_HMS_VERSION}', HEC_HMS_VERSION)
    current_date_time = datetime.now()
    last_modified_date = current_date_time.strftime('%d %B %Y')
    last_modified_time = current_date_time.strftime('%H:%M')

    control_file = control_file.replace('{LAST_MODIFIED_DATE}', last_modified_date)
    control_file = control_file.replace('{LAST_MODIFIED_TIME}', last_modified_time)

    start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')

    start_date = start_datetime.strftime('%d %B %Y')
    start_time = start_datetime.strftime('%H:%M')
    end_date = end_datetime.strftime('%d %B %Y')
    end_time = end_datetime.strftime('%H:%M')

    control_file = control_file.replace('{START_DATE}', start_date)
    control_file = control_file.replace('{START_TIME}', start_time)
    control_file = control_file.replace('{END_DATE}', end_date)
    control_file = control_file.replace('{END_TIME}', end_time)

    time_interval = str(int(((end_datetime-start_datetime).total_seconds())/60))
    control_file = control_file.replace('{TIME_INTERVAL}', time_interval)

    with open(control_file_path, 'w') as file:
        file.write(control_file)
        file.write('\n\n')
    file.close()


def create_control_file(model_name, start_datetime, end_datetime):
    control_file_path = CONTROL_FILE_NAME.replace('{MODEL_NAME}', model_name)
    control_file = CONTROL_TEMPLATE.replace('{MODEL_NAME}', model_name)
    control_file = control_file.replace('{HEC_HMS_VERSION}', HEC_HMS_VERSION)
    current_date_time = datetime.now()
    last_modified_date = current_date_time.strftime('%d %B %Y')
    last_modified_time = current_date_time.strftime('%H:%M')

    control_file = control_file.replace('{LAST_MODIFIED_DATE}', last_modified_date)
    control_file = control_file.replace('{LAST_MODIFIED_TIME}', last_modified_time)

    start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')

    start_date = start_datetime.strftime('%d %B %Y')
    start_time = start_datetime.strftime('%H:%M')
    end_date = end_datetime.strftime('%d %B %Y')
    end_time = end_datetime.strftime('%H:%M')

    control_file = control_file.replace('{START_DATE}', start_date)
    control_file = control_file.replace('{START_TIME}', start_time)
    control_file = control_file.replace('{END_DATE}', end_date)
    control_file = control_file.replace('{END_TIME}', end_time)

    time_interval = str(int(((end_datetime-start_datetime).total_seconds())/60))
    control_file = control_file.replace('{TIME_INTERVAL}', time_interval)

    with open(control_file_path, 'w') as file:
        file.write(control_file)
        file.write('\n\n')
    file.close()





