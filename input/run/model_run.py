from config import RUN_FILE_TEMPLATE, RUN_FILE_NAME
from datetime import datetime


def create_run_file(model_name, date_time):
    run_file_path = RUN_FILE_NAME.replace('{MODEL_NAME}', model_name)
    run_file = RUN_FILE_TEMPLATE.replace('{MODEL_NAME}', model_name)
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

    with open(run_file_path, 'w') as file:
        file.write(run_file)
        file.write('\n\n')
    file.close()
