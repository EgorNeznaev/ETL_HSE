from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 2)
}


def get_latest_date_from_db():
    pg_hook = PostgresHook(postgres_conn_id='airflow_metadata_db')

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(noted_date) FROM iot_all_data")
            result = cursor.fetchone()
            return result[0] if result[0] else None


def process_incremental_data(**context):
    last_date = get_latest_date_from_db()

    if last_date:
        start_date = (last_date - timedelta(days=3)).strftime('%d-%m-%Y')
    else:
        start_date = None

    file_path = '/opt/airflow/dags/IOT-temp.csv'
    df = pd.read_csv(file_path)

    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])

    if start_date:
        start_date_dt = pd.to_datetime(start_date, format='%d-%m-%Y')
        df = df[df['noted_date'] >= start_date_dt].copy()

    df = df.rename(columns={'room_id/id': 'room_id', 'out/in': 'out_in'})
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

    context['ti'].xcom_push(key='incremental_data', value=final_df.to_json(orient='records'))
    return f"Обработано {len(final_df)} записей для инкрементальной загрузки"


def load_incremental_data(**context):
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='process_incremental_data', key='incremental_data')

    if not data_json:
        return "Нет новых данных для загрузки"

    final_df = pd.read_json(data_json, orient='records')

    if final_df.empty:
        return "Нет данных для загрузки"

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

            sql_insert = """
                         INSERT INTO iot_all_data (room_id, noted_date, temp, out_in)
                         SELECT %(room_id)s, %(noted_date)s, %(temp)s, %(out_in)s 
                            WHERE NOT EXISTS ( SELECT 1 FROM iot_all_data 
                                                WHERE room_id = %(room_id)s
                                                AND noted_date = %(noted_date)s
                                                AND temp = %(temp)s
                                                AND out_in = %(out_in)s
                                                ) 
                         """

            inserted_count = 0
            records = final_df.to_dict('records')

            for record in records:
                cursor.execute(sql_insert, record)
                inserted_count += cursor.rowcount

            conn.commit()

    return f"Инкрементальная загрузка: добавлено {inserted_count} новых записей"


incremental_dag = DAG(
    'incremental',
    default_args=default_args,
    description='Инкрементальная загрузка данных IoT за последние дни',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['iot', 'incremental'],
    max_active_runs=1
)

start_task = DummyOperator(task_id='start', dag=incremental_dag)
end_task = DummyOperator(task_id='end', dag=incremental_dag)

process_task = PythonOperator(
    task_id='process_incremental_data',
    python_callable=process_incremental_data,
    dag=incremental_dag,
    provide_context=True
)

load_task = PythonOperator(
    task_id='load_incremental_data',
    python_callable=load_incremental_data,
    dag=incremental_dag,
    provide_context=True
)

start_task >> process_task >> load_task >> end_task