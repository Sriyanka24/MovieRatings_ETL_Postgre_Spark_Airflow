import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transformation import *


#define the ETL function
def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'email': ['info@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}


# initiate the DAG

dag = DAG(dag_id = "etl_pipeline",
          default_args = default_args,
          schedule_interval = "0 0 * * *"
          )

#define the etl task
etl_task = PythonOperator(task_id = "etl_task",
                          python_callable=etl,
                          dag=dag
                          )

etl()