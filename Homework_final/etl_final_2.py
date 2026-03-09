from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pymongo
import psycopg2
import json
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}
def replicate_all_collections(**context):
    print("НАЧАЛО ETL ПРОЦЕССА")
    try:
        mongo_conn = BaseHook.get_connection('mongo_default')
        mongo_uri = f"mongodb://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}:{mongo_conn.port}/etl?authSource=admin"
        
        mongo_client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        mongo_client.admin.command('ping')
        print("MongoDB подключена")        
        db = mongo_client['etl']
        all_collections = [c for c in db.list_collection_names() if not c.startswith('system.')]
        print(f"Найдено коллекций: {len(all_collections)}")
        print(f"Список: {all_collections}")
        
    except Exception as e:
        print(f"Ошибка MongoDB: {e}")
        raise
    try:
        pg_conn = BaseHook.get_connection('etl_postgres')
        pg_client = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port,
        database=pg_conn.schema or 'airflow',
        user=pg_conn.login,
        password=pg_conn.password
    )
        cursor = pg_client.cursor()
        print("PostgreSQL подключена")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS stg;")
        pg_client.commit()
        
    except Exception as e:
        print(f"Ошибка PostgreSQL: {e}")
        raise
    results = {}
    
    for collection_name in all_collections:
        print(f"\n--- Обработка коллекции: {collection_name} ---")
        
        try:
            collection = db[collection_name]
            documents = list(collection.find({}))
            doc_count = len(documents)
            print(f"Получено {doc_count} документов")
            
            if doc_count == 0:
                print(f"Коллекция {collection_name} пуста")
                results[collection_name] = 0
                continue
            if documents:
                sample = documents[0]
                fields = list(sample.keys())
                fields = [f for f in fields if not f.startswith('_')]
                print(f"Поля: {fields}")
            table_name = f"stg.{collection_name}"
            if collection_name == 'user_sessions':
                cursor.execute("DROP TABLE IF EXISTS stg.user_sessions CASCADE;")
                cursor.execute("""
                CREATE TABLE stg.user_sessions (
                    session_id VARCHAR(50) PRIMARY KEY,
                    user_id VARCHAR(50),
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    pages_visited JSONB,
                    device JSONB,
                    actions JSONB,
                    session_duration INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                loaded = 0
                for doc in documents:
                    try:
                        session_id = doc.get('session_id')
                        user_id = doc.get('user_id')
                        
                        start_time = doc.get('start_time', '').replace('Z', '+00:00')
                        end_time = doc.get('end_time', '').replace('Z', '+00:00')
                        
                        # Расчет длительности
                        try:
                            start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                            end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                            duration = int((end - start).total_seconds())
                        except:
                            duration = None
                        
                        pages = json.dumps(doc.get('pages_visited', []))
                        actions = json.dumps(doc.get('actions', []))
                        device = json.dumps(doc.get('device', {}))
                        
                        cursor.execute("""
                        INSERT INTO stg.user_sessions 
                        (session_id, user_id, start_time, end_time, pages_visited, device, actions, session_duration)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (session_id) DO UPDATE SET
                            end_time = EXCLUDED.end_time,
                            pages_visited = EXCLUDED.pages_visited,
                            actions = EXCLUDED.actions,
                            session_duration = EXCLUDED.session_duration
                        """, (session_id, user_id, start_time, end_time, pages, device, actions, duration))
                        
                        loaded += 1
                        if loaded % 100 == 0:
                            pg_client.commit()
                            print(f"   Загружено {loaded}...")
                            
                    except Exception as e:
                        print(f"Ошибка: {e}")
                
                results[collection_name] = loaded
                print(f"Загружено {loaded} сессий")
                      
            
            elif collection_name == 'support_tickets':
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS stg.support_tickets (
                    ticket_id VARCHAR(50) PRIMARY KEY,
                    user_id VARCHAR(50),
                    status VARCHAR(20),
                    issue_type VARCHAR(50),
                    messages JSONB,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    resolution_time INTEGER,
                    created_dt DATE
                );
                """)
                
                loaded = 0
                for doc in documents:
                    try:
                        ticket_id = doc.get('ticket_id')
                        user_id = doc.get('user_id')
                        status = doc.get('status')
                        issue_type = doc.get('issue_type')
                        
                        created_at = doc.get('created_at', '').replace('Z', '+00:00')
                        updated_at = doc.get('updated_at', '').replace('Z', '+00:00')
                        
                        try:
                            created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            updated = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                            resolution = int((updated - created).total_seconds())
                        except:
                            resolution = None
                        
                        messages = json.dumps(doc.get('messages', []))
                        created_dt = created_at[:10] if created_at else None
                        
                        cursor.execute("""
                        INSERT INTO stg.support_tickets 
                        (ticket_id, user_id, status, issue_type, messages, created_at, updated_at, resolution_time, created_dt)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticket_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            updated_at = EXCLUDED.updated_at,
                            resolution_time = EXCLUDED.resolution_time
                        """, (ticket_id, user_id, status, issue_type, messages, created_at, updated_at, resolution, created_dt))
                        
                        loaded += 1
                    except Exception as e:
                        print(f"Ошибка: {e}")
                
                results[collection_name] = loaded
                print(f"Загружено {loaded} тикетов")
            
            elif collection_name == 'event_logs':
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS stg.event_logs (
                    event_id VARCHAR(50) PRIMARY KEY,
                    event_time TIMESTAMP,
                    event_type VARCHAR(50),
                    user_id VARCHAR(50),
                    details JSONB
                );
                """)
                
                loaded = 0
                for doc in documents:
                    try:
                        event_id = doc.get('event_id')
                        event_time = doc.get('timestamp', '').replace('Z', '+00:00')
                        event_type = doc.get('event_type')
                        user_id = doc.get('user_id')
                        details = json.dumps(doc.get('details', {}))
                        
                        cursor.execute("""
                        INSERT INTO stg.event_logs 
                        (event_id, event_time, event_type, user_id, details)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                        """, (event_id, event_time, event_type, user_id, details))
                        
                        loaded += 1
                    except Exception as e:
                        print(f"Ошибка: {e}")
                
                results[collection_name] = loaded
                print(f"Загружено {loaded} событий")
            
            elif collection_name == 'user_recommendations':
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS stg.user_recommendations (
                    user_id VARCHAR(50) PRIMARY KEY,
                    recommended_products JSONB,
                    last_updated TIMESTAMP
                );
                """)
                
                loaded = 0
                for doc in documents:
                    try:
                        user_id = doc.get('user_id')
                        products = json.dumps(doc.get('recommended_products', []))
                        last_updated = doc.get('last_updated', '').replace('Z', '+00:00')
                        
                        cursor.execute("""
                        INSERT INTO stg.user_recommendations 
                        (user_id, recommended_products, last_updated)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            recommended_products = EXCLUDED.recommended_products,
                            last_updated = EXCLUDED.last_updated
                        """, (user_id, products, last_updated))
                        
                        loaded += 1
                    except Exception as e:
                        print(f"Ошибка: {e}")
                
                results[collection_name] = loaded
                print(f"Загружено {loaded} рекомендаций")
            
            elif collection_name == 'moderation_queue':
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS stg.moderation_queue (
                    review_id VARCHAR(50) PRIMARY KEY,
                    user_id VARCHAR(50),
                    product_id VARCHAR(50),
                    review_text TEXT,
                    rating INTEGER,
                    moderation_status VARCHAR(20),
                    flags JSONB,
                    submitted_at TIMESTAMP
                );
                """)
                
                loaded = 0
                for doc in documents:
                    try:
                        review_id = doc.get('review_id')
                        user_id = doc.get('user_id')
                        product_id = doc.get('product_id')
                        review_text = doc.get('review_text')
                        rating = doc.get('rating')
                        status = doc.get('moderation_status')
                        flags = json.dumps(doc.get('flags', []))
                        submitted_at = doc.get('submitted_at', '').replace('Z', '+00:00')
                        
                        cursor.execute("""
                        INSERT INTO stg.moderation_queue 
                        (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (review_id) DO UPDATE SET
                            moderation_status = EXCLUDED.moderation_status,
                            flags = EXCLUDED.flags
                        """, (review_id, user_id, product_id, review_text, rating, status, flags, submitted_at))
                        
                        loaded += 1
                    except Exception as e:
                        print(f"Ошибка: {e}")
                
                results[collection_name] = loaded
                print(f"Загружено {loaded} отзывов")
            pg_client.commit()
            
        except Exception as e:
            print(f"Ошибка при обработке {collection_name}: {e}")
            pg_client.rollback()
            results[collection_name] = f"Ошибка: {str(e)[:50]}"
    print("СОЗДАНИЕ АНАЛИТИЧЕСКИХ ВИТРИН")
    
    try:
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS mart;

        DROP TABLE IF EXISTS mart.user_activity_mart;

        CREATE TABLE mart.user_activity_mart AS
        SELECT 
            DATE(start_time) AS activity_date,
            user_id,
            COUNT(*) AS session_count,
            SUM(COALESCE(session_duration, 0)) AS total_seconds,
            AVG(COALESCE(session_duration, 0)) AS avg_session_seconds,
            AVG(jsonb_array_length(COALESCE(pages_visited, '[]'::jsonb))) AS avg_pages_per_session,
            AVG(jsonb_array_length(COALESCE(actions, '[]'::jsonb))) AS avg_actions_per_session,
            COUNT(*) FILTER (WHERE device->>'type' = 'mobile') AS mobile_sessions,
            COUNT(*) FILTER (WHERE device->>'type' = 'desktop') AS desktop_sessions,
            CURRENT_TIMESTAMP AS calculated_at
        FROM stg.user_sessions
        WHERE start_time IS NOT NULL
        GROUP BY DATE(start_time), user_id;
        """)
        print("Витрина user_activity_mart создана")
        cursor.execute("""
        DROP TABLE IF EXISTS mart.support_efficiency_mart;
        
        CREATE TABLE mart.support_efficiency_mart AS
        SELECT 
            created_dt,
            issue_type,
            status,
            COUNT(*) AS ticket_count,
            AVG(COALESCE(resolution_time, 0)) AS avg_resolution_seconds,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_tickets,
            SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_tickets,
            CURRENT_TIMESTAMP AS calculated_at
        FROM stg.support_tickets
        WHERE created_dt IS NOT NULL
        GROUP BY created_dt, issue_type, status;
        
        CREATE INDEX idx_support_efficiency_date ON mart.support_efficiency_mart(created_dt);
        """)
        print("Витрина support_efficiency_mart создана")
        cursor.execute("""
        DROP TABLE IF EXISTS mart.moderation_mart;
        
        CREATE TABLE mart.moderation_mart AS
        SELECT 
            DATE(submitted_at) AS review_date,
            moderation_status,
            COUNT(*) AS review_count,
            AVG(rating) AS avg_rating,
            SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
            SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS negative_reviews
        FROM stg.moderation_queue
        WHERE submitted_at IS NOT NULL
        GROUP BY DATE(submitted_at), moderation_status;
        """)
        print("Витрина moderation_mart создана")
        
        pg_client.commit()
        
    except Exception as e:
        print(f"Ошибка создания витрин: {e}")
        pg_client.rollback()
    print("ИТОГИ РЕПЛИКАЦИИ")
    
    total_docs = 0
    for coll, count in results.items():
        if isinstance(count, int):
            print(f"{coll}: {count} документов")
            total_docs += count
        else:
            print(f"{coll}: {count}")
    
    print(f"\nВСЕГО ЗАГРУЖЕНО: {total_docs} документов")
    
    cursor.close()
    pg_client.close()
    mongo_client.close()
    print("ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО")
    
    return f"Загружено {total_docs} документов из {len(results)} коллекций"

with DAG(
    'etl_final_all_collections',
    default_args=default_args,
    description='ИТОГОВЫЙ ETL: Все коллекции MongoDB -> PostgreSQL -> Витрины',
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:
    
    replicate_all = PythonOperator(
        task_id='replicate_all_collections',
        python_callable=replicate_all_collections
    )