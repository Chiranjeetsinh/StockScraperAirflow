from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pipeline_1.utils import fetch_data,preprocess_date,generate_sentiment_scores,store_data
import requests


def mock_api_alert(context):
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
    'pipeline_1',
    default_args=default_args,
    description='Pipeline 1 DAG',
    schedule_interval='0 19 * * 1-5',
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

preprocess_data_task = PythonOperator(
    task_id='preprocess_date',
    python_callable=preprocess_date,
    provide_context=True,
    dag=dag,
)

generate_sentiment_score_task = PythonOperator(
    task_id='generate_sentiment_score',
    python_callable=generate_sentiment_scores,
    provide_context=True,
    dag=dag,
)

persist_data_task = PythonOperator(
    task_id='persist_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

trigger_pipeline_2 = TriggerDagRunOperator(
    task_id='trigger_pipeline_2',
    trigger_dag_id='pipeline_2',
    dag=dag,
)

fetch_data_task >> preprocess_data_task >> generate_sentiment_score_task >> persist_data_task >> trigger_pipeline_2


