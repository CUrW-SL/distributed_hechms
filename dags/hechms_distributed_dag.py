from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

prod_dag_name = 'distributed-hec-hms-dag'
queue = 'default'
schedule_interval = '15 * * * *'
dag_pool = 'curw_prod_runs'

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0, hour=2),
    'email': ['hasithadkr7.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': queue,
    'catchup': False,
}

create_input_cmd = 'curl -X GET "http://10.138.0.3:5000/HECHMS/distributed/init/{{ (execution_date).strftime(\"%Y-%m-%d %H:00:00\") }}/3/2/1'

run_hechms_preprocess_cmd = 'curl -X GET "http://10.138.0.3:5000/HECHMS/distributed/pre-process/{{ (execution_date).strftime(\"%Y-%m-%d %H:00:00\") }}/3/2'

run_hechms_cmd = 'curl -X GET "10.138.0.3:5000/HECHMS/distributed/run"'

run_hechms_postprocess_cmd = '10.138.0.3:5000/HECHMS/distributed/post-process/{{ (execution_date).strftime(\"%Y-%m-%d %H:00:00\") }}/3/2'

upload_discharge_cmd = "echo 'upload discharge data'"

with DAG(dag_id=prod_dag_name, default_args=default_args, schedule_interval=schedule_interval,
         description='Run HEC-HMS distributed DAG') as dag:
    create_input = BashOperator(
        task_id='create_input',
        bash_command=create_input_cmd,
        dag=dag,
        pool=dag_pool,
    )

    run_hechms_preprocess = BashOperator(
        task_id='run_hechms_preprocess',
        bash_command=run_hechms_preprocess_cmd,
        dag=dag,
        pool=dag_pool,
    )

    run_hechms = BashOperator(
        task_id='run_hechms',
        bash_command=run_hechms_cmd,
        dag=dag,
        pool=dag_pool,
    )

    run_hechms_postprocess = BashOperator(
        task_id='run_hechms_postprocess',
        bash_command=run_hechms_postprocess_cmd,
        dag=dag,
        pool=dag_pool,
    )

    upload_discharge = BashOperator(
        task_id='upload_dischargew',
        bash_command=upload_discharge_cmd,
        dag=dag,
        pool=dag_pool,
    )

    create_input >> run_hechms_preprocess >> run_hechms >> run_hechms_postprocess >> upload_discharge

