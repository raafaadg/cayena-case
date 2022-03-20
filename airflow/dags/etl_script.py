from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from scripts.web_scraping import WebScraping
from os import path

queires_path = path.join(path.abspath(path.dirname(__file__)), 'queries')
ws = WebScraping(queires_path)

DEFAULT_ARGS = {
    'owner': 'cayena',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 17),
}

dag = DAG('web_scraping_pipeline',
          default_args=DEFAULT_ARGS,
          schedule_interval=None
          )
    
start_task = DummyOperator(
    task_id="start_pipeline",
    dag=dag
)

local_resource_task = BashOperator(
    task_id="create_local_auxiliar_resources",
    bash_command='mkdir pickle',
    dag=dag
)

resources_task = PythonOperator(
    task_id='create_database_resources',
    provide_context=True,
    python_callable=ws.create_resources_task,
    retries=3,
    dag=dag
)

scrap_books_from_web = PythonOperator(
    task_id='scrap_books_from_web',
    provide_context=True,
    python_callable=ws.get_books_url_task,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_postgres',
    provide_context=True,
    python_callable=ws.upload_data_to_postgres_task,
    dag=dag
)

result_task = PythonOperator(
    task_id="run_results_query",
    provide_context=True,
    python_callable=ws.execute_result_query,
    dag=dag
)

clean_local_resource_task = BashOperator(
    task_id="clean_local_auxiliar_resources",
    bash_command='rm -rf /temp/*',
    dag=dag
)

finish_task = DummyOperator(
    task_id="finish_pipeline",
    dag=dag
)

start_task >> local_resource_task >> resources_task >> \
scrap_books_from_web >> load_task >> result_task >> \
clean_local_resource_task >> finish_task