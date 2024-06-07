import requests
import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from util import extract_data, transform_data, load_data, load_data_to_postgresql


default_args = {"owner": "Chichi",
                "email": ["chinyere.nwigwe126@gmail.com"],
                "retries": 2,
                "retry_delay": timedelta(seconds=3)
                }


with DAG (dag_id = "simple_etl",
          schedule = "0 */12 * * *",
          catchup = False,
          start_date = datetime(2024, 6, 7),
          default_args = default_args
          ) as dag:
    
 
    
    first_task = PythonOperator( task_id = "getting_data",
                           python_callable = extract_data,
                           dag = dag)
    
    second_task = PythonOperator( task_id = "cleaning_data",
                           python_callable = transform_data,
                           dag = dag)
    
    third_task = PythonOperator( task_id='loading_data',
                      python_callable=load_data,
                      dag=dag)
    
    final_task = PythonOperator( task_id='loading_data',
                      python_callable=load_data_to_postgresql,
                      dag=dag)

    

    first_task>>second_task >> third_task >> final_task

