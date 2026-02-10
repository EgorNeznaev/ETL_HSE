from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}


def process_full_data(**context):
    file_path = '/opt/airflow/dags/IOT-temp.csv'
    df = pd.read_csv(file_path)

    df = df.rename(columns={'room_id/id': 'room_id', 'out/in': 'out_in'})
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])
    df['date_only'] = df['noted_date'].dt.date

    q_low = df['temp'].quantile(0.05)
    q_high = df['temp'].quantile(0.95)
    df_filtered = df[(df['temp'] >= q_low) & (df['temp'] <= q_high)].copy()
    in_data = df_filtered[df_filtered['out_in'] == 'In'].copy()

    final_df = pd.DataFrame({
        'room_id': in_data['room_id'],
        'noted_date': pd.to_datetime(in_data['date_only']).dt.strftime('%Y-%m-%d'),
        'temp': in_data['temp'].astype(int),
        'out_in': in_data['out_in']
    })

    final_df = final_df.drop_duplicates(subset=['room_id', 'noted_date', 'temp', 'out_in'])

    context['ti'].xcom_push(key='full_data', value=final_df.to_json(orient='records'))
    return f"Обработано {len(final_df)} записей для полной загрузки"


def load_full_data(**context):
    ti = context['ti']

    data_json = ti.xcom_pull(task_ids='process_full_data', key='full_data')

    if not data_json:
        return "Нет данных для загрузки"

    final_df = pd.read_json(data_json, orient='records')

    pg_hook = PostgresHook(postgres_conn_id='airflow_metadata_db')

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS iot_all_data
                           (
                               id
                               SERIAL
                               PRIMARY
                               KEY,
                               room_id
                               TEXT,
                               noted_date
                               DATE,
                               temp
                               INTEGER,
                               out_in
                               TEXT,
                               load_timestamp
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           """)

            cursor.execute("TRUNCATE TABLE iot_all_data RESTART IDENTITY")

            sql_insert = """
                         INSERT INTO iot_all_data (room_id, noted_date, temp, out_in)
                         VALUES (%(room_id)s, %(noted_date)s, %(temp)s, %(out_in)s) \
                         """

            records = final_df.to_dict('records')
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor.executemany(sql_insert, batch)
                total_inserted += len(batch)

            conn.commit()

    return f"Полная загрузка: успешно загружено {total_inserted} записей"


full_load_dag = DAG(
    'full',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['iot', 'simple_load', 'no_grouping'],
    max_active_runs=1
)

start_task = DummyOperator(task_id='start', dag=full_load_dag)
end_task = DummyOperator(task_id='end', dag=full_load_dag)

process_task = PythonOperator(
    task_id='process_full_data',
    python_callable=process_full_data,
    dag=full_load_dag,
    provide_context=True
)

load_task = PythonOperator(
    task_id='load_full_data',
    python_callable=load_full_data,
    dag=full_load_dag,
    provide_context=True
)

start_task >> process_task >> load_task >> end_task