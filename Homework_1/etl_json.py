from datetime import datetime, timedelta
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

default_args = {
    'owner': 'Egor N.',
    'depends_on_past': False,
    'start_date': datetime(2025,1,24),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'etl_pets_json',
    default_args=default_args,
    description='ETL for pets from json',
    schedule_interval='@daily',
    catchup=False,
    tags=['pets']
)

JSON_DATA = {
  "pets": [
    {
      "name" : "Purrsloud",
      "species" : "Cat",
      "favFoods" : ["wet food", "dry food", "<strong>any</strong> food"],
      "birthYear" : 2016,
      "photo" : "https://learnwebcode.github.io/json-example/images/cat-2.jpg"
    },
    {
      "name" : "Barksalot",
      "species" : "Dog",
      "birthYear" : 2008,
      "photo" : "https://learnwebcode.github.io/json-example/images/dog-1.jpg"
    },
    {
      "name" : "Meowsalot",
      "species" : "Cat",
      "favFoods" : ["tuna", "catnip", "celery"],
      "birthYear" : 2012,
      "photo" : "https://learnwebcode.github.io/json-example/images/cat-1.jpg"
    }
  ]
}

def extract_json(**kwargs):
    logging.info(f'Extracting JSON data')
    pets_data = JSON_DATA.get('pets',[])
    
    return pets_data

def transform_json(**kwargs):
    ti = kwargs['ti']
    pets_data = JSON_DATA.get('pets',[])
    linear_data = []
    for pet in pets_data:
        linear = {
            'name': pet.get('name'),
            'species': pet.get('species'),
            'birthYear': pet.get('birthYear'),
            'photo': pet.get('photo')
        }
        fav_foods = pet.get('favFoods', [])
        if fav_foods:
            linear['favFoods'] = ', '.join(fav_foods)
            linear['favFoods'] = linear['favFoods'].replace('<strong>', '').replace('</strong>', '')
        else:
            linear['favFoods'] = ''
        linear_data.append(linear)
    logging.info('Transforming JSON data')
    return linear_data

def save_csv(**kwargs):
    ti = kwargs['ti']
    linear_data = ti.xcom_pull(task_ids='transform_json', key='return_value')
    df = pd.DataFrame(linear_data)
    output_path = '/opt/airflow/dags/pets.csv'
    df.to_csv(output_path,index=False, encoding='utf-8')
    logging.info(f'Saving CSV file in {output_path}')
    return output_path

start_task = DummyOperator(task_id='start', dag=dag)
extraxt_task = PythonOperator(
    task_id='extract_json',
    python_callable=extract_json,
    dag=dag
)
transform_task = PythonOperator(
    task_id='transform_json',
    python_callable=transform_json,
    dag=dag
)
save_task = PythonOperator(
    task_id='save_csv',
    python_callable=save_csv,
    dag=dag
)
end_task = DummyOperator(task_id='end', dag=dag)