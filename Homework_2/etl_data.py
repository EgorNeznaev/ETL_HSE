from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'Egpr N',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

dag = DAG(
    'etl_data',
    default_args=default_args,
    description='Обработка данных',
    schedule_interval=None,
    catchup=False,
)

def process_data():
    print("=" * 60)
    print("ОБРАБОТКА ДАННЫХ")
    print("=" * 60)
    input_file = '/opt/airflow/dags/IOT-temp.csv'
    
    print(f"📁 Исходный файл: {input_file}")
    if not os.path.exists(input_file):
        print(f"Файл не найден: {input_file}")
        return
    
    try:
        print("Чтение данных...")
        df = pd.read_csv(input_file)
        print(f"Прочитано строк: {len(df)}")
        print(f"   Колонки: {list(df.columns)}")
        original_count = len(df)
        print("\nФильтрация: out/in = 'In'...")
        df['out/in'] = df['out/in'].astype(str).str.strip()
        df_filtered = df[df['out/in'].str.lower() == 'in'].copy()
        print(f"После фильтрации: {len(df_filtered)} строк (удалено {original_count - len(df_filtered)})")
        
        if len(df_filtered) == 0:
            print("Нет данных после фильтрации!")
            return
        print("\nПреобразование даты...")
        df_filtered['noted_date'] = pd.to_datetime(
            df_filtered['noted_date'], 
            format='%d-%m-%Y %H:%M',
            errors='coerce'
        )

        invalid_dates = df_filtered['noted_date'].isnull().sum()
        if invalid_dates > 0:
            print(f"Удалено строк с некорректными датами: {invalid_dates}")
            df_filtered = df_filtered.dropna(subset=['noted_date'])
        
        df_filtered['noted_date_str'] = df_filtered['noted_date'].dt.strftime('%Y-%m-%d')
        df_filtered['noted_date'] = df_filtered['noted_date'].dt.date
        
        print(f"Даты преобразованы. Пример: {df_filtered['noted_date_str'].iloc[0]}")
        
        print("\nОчистка температуры...")
        lower_percentile = df_filtered['temp'].quantile(0.05)
        upper_percentile = df_filtered['temp'].quantile(0.95)
        
        print(f"   5-й процентиль: {lower_percentile:.2f}°C")
        print(f"   95-й процентиль: {upper_percentile:.2f}°C")
        
        before_clean = len(df_filtered)
        df_cleaned = df_filtered[
            (df_filtered['temp'] >= lower_percentile) & 
            (df_filtered['temp'] <= upper_percentile)
        ].copy()
        
        print(f"После очистки: {len(df_cleaned)} строк (удалено {before_clean - len(df_cleaned)})")
        df_cleaned['date_for_group'] = df_cleaned['noted_date_str']
        daily_stats = df_cleaned.groupby('date_for_group').agg({
            'temp': ['mean', 'min', 'max', 'count']
        }).round(2)
        
        daily_stats.columns = ['avg_temp', 'min_temp', 'max_temp', 'readings_count']
        daily_stats = daily_stats.reset_index()
        hottest_days = daily_stats.nlargest(5, 'avg_temp')[['date_for_group', 'avg_temp', 'readings_count']]
        hottest_days = hottest_days.rename(columns={'date_for_group': 'date', 'avg_temp': 'temperature'})
        coldest_days = daily_stats.nsmallest(5, 'avg_temp')[['date_for_group', 'avg_temp', 'readings_count']]
        coldest_days = coldest_days.rename(columns={'date_for_group': 'date', 'avg_temp': 'temperature'})
        
        print("\n5 САМЫХ ЖАРКИХ ДНЕЙ:")
        for idx, row in hottest_days.iterrows():
            print(f"   {row['date']}: {row['temperature']}°C (замеров: {row['readings_count']})")
        
        print("\n5 САМЫХ ХОЛОДНЫХ ДНЕЙ:")
        for idx, row in coldest_days.iterrows():
            print(f"   {row['date']}: {row['temperature']}°C (замеров: {row['readings_count']})")
        
        results_dir = '/opt/airflow/dags/results'
        os.makedirs(results_dir, exist_ok=True)
        
        cleaned_data_path = os.path.join(results_dir, 'cleaned_data.csv')
        df_cleaned.to_csv(cleaned_data_path, index=False)
        print(f"Очищенные данные: {cleaned_data_path} ({len(df_cleaned)} строк)")
        
        hottest_path = os.path.join(results_dir, 'hottest_days.csv')
        hottest_days.to_csv(hottest_path, index=False)
        print(f"Самые жаркие дни: {hottest_path}")
        coldest_path = os.path.join(results_dir, 'coldest_days.csv')
        coldest_days.to_csv(coldest_path, index=False)
        print(f"Самые холодные дни: {coldest_path}")
        daily_stats_path = os.path.join(results_dir, 'daily_statistics.csv')
        daily_stats.to_csv(daily_stats_path, index=False)
        print(f"Ежедневная статистика: {daily_stats_path}")
        print("\n" + "=" * 60)
        print("ИТОГОВЫЙ ОТЧЕТ:")
        print("=" * 60)
        print(f"Исходных строк: {original_count}")
        print(f"После фильтрации 'In': {before_clean}")
        print(f"После очистки по процентилям: {len(df_cleaned)}")
        print(f"Уникальных дней в данных: {daily_stats.shape[0]}")
        print(f"\nФайлы сохранены в папке: {results_dir}")
        
        print("\nСодержимое папки results:")
        for file in os.listdir(results_dir):
            file_path = os.path.join(results_dir, file)
            size = os.path.getsize(file_path)
            print(f"  - {file} ({size} байт)")
        
    except Exception as e:
        print(f"ОШИБКА: {e}")
        import traceback
        print(traceback.format_exc())
    
    print("\n" + "=" * 60)
    print("ОБРАБОТКА ЗАВЕРШЕНА!")
    print("=" * 60)

process_task = PythonOperator(
    task_id='process_and_save_results',
    python_callable=process_data,
    dag=dag,
)