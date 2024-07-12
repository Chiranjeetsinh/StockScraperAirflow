from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pipeline_2.analytics import find_mean_age_by_occupation, find_top_20_highest_rated_movies, find_top_genres_by_occupation_and_age_group, find_top_similar_movies
import pandas as pd
import requests


def mock_api_alert(context):
    print("sending alert since task failed")
    print(context)
    task_instance = context['task_instance']
    alert_message = {
        "task_id": task_instance.task_id,
        "dag_id": task_instance.dag_id,
        "execution_date": str(context['execution_date']),
        "log_url": task_instance.log_url,
    }
    response = requests.post('https://ptsv3.com/t/cjsinh/', json=alert_message)
    if response.status_code != 200:
        print(f"Failed to send alert: {response.content}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': mock_api_alert
}



dag = DAG(
    'pipeline_2',
    default_args=default_args,
    description='Pipeline 2 DAG',
    schedule_interval='0 20 * * 1-5',
)

mean_age_task = PythonOperator(
    task_id='mean_age_per_occupation',
    python_callable=find_mean_age_by_occupation,
    provide_context=True,
    dag=dag,
)

top_20_movies_task = PythonOperator(
    task_id='top_20_highest_rated_movies',
    python_callable=find_top_20_highest_rated_movies,
    provide_context=True,
    dag=dag,
)

top_genres_task = PythonOperator(
    task_id='top_genres_by_occupation_age_group',
    python_callable=find_top_genres_by_occupation_and_age_group,
    provide_context=True,
    dag=dag,
)

top_similar_movies_task = PythonOperator(
    task_id='top_10_similar_movies',
    python_callable=find_top_similar_movies,
    op_kwargs={'movie_id': 2},
    provide_context=True,
    dag=dag,
)


mean_age_task >> top_20_movies_task >> top_genres_task >> top_similar_movies_task