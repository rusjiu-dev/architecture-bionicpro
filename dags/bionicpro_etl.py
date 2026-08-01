from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import logging

# Конфигурация
default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@bionicpro.com']
}

dag = DAG(
    'bionicpro_etl',
    default_args=default_args,
    description='ETL для витрины отчётов BionicPRO',
#    schedule_interval='0 2 * * *',  # Ежедневно в 02:00
    schedule_interval=timedelta(seconds=30),  # Каждые 30 секунд
    catchup=False,
    max_active_runs=1,
    tags=['bionicpro', 'etl', 'reports']
)

# SQL-запросы для извлечения данных из CRM (источник)
CRM_EXTRACT_SQL = """
SELECT 
    u.id AS user_id,
    u.full_name,
    u.email,
    u.phone,
    u.registration_date,
    p.id AS prosthesis_id,
    p.model,
    p.purchase_date,
    p.warranty_end_date
FROM crm.users u
JOIN crm.prostheses p ON u.id = p.user_id
WHERE p.is_active = true
  AND u.registration_date >= '2025-01-01'
"""

# SQL-запросы для извлечения данных телеметрии (источник)
TELEMETRY_EXTRACT_SQL = """
SELECT 
    t.user_id,
    t.prosthesis_id,
    DATE(t.timestamp) AS date,
    COUNT(*) AS gesture_count,
    AVG(t.response_time_ms) AS avg_response_time_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.response_time_ms) AS p95_response_time_ms,
    SUM(CASE WHEN t.is_error THEN 1 ELSE 0 END) AS error_count,
    MAX(t.battery_level) AS max_battery_level,
    MIN(t.battery_level) AS min_battery_level,
    SUM(t.active_seconds) / 60 AS active_minutes
FROM telemetry.raw_events t
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY t.user_id, t.prosthesis_id, DATE(t.timestamp)
"""

# Создание витрины (целевая таблица)
CREATE_MART_SQL = """
CREATE TABLE IF NOT EXISTS reports.mart_daily_user_report (
    user_id UUID NOT NULL,
    prosthesis_id UUID NOT NULL,
    user_full_name VARCHAR(255),
    user_email VARCHAR(255),
    prosthesis_model VARCHAR(100),
    gesture_count INTEGER DEFAULT 0,
    avg_response_time_ms FLOAT,
    p95_response_time_ms FLOAT,
    error_count INTEGER DEFAULT 0,
    error_rate FLOAT GENERATED ALWAYS AS (error_count::float / NULLIF(gesture_count, 0)) STORED,
    battery_health_score FLOAT,
    active_minutes INTEGER DEFAULT 0,
    report_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, prosthesis_id, report_generated_at)
);

CREATE INDEX IF NOT EXISTS idx_mart_user_date ON reports.mart_daily_user_report (user_id);
CREATE INDEX IF NOT EXISTS idx_mart_date ON reports.mart_daily_user_report (report_generated_at DESC);
"""

# Функции для задач DAG

def extract_crm(**context):
    """Извлечение данных из CRM"""
    pg_hook = PostgresHook(postgres_conn_id='crm_db')
    connection = pg_hook.get_conn()
    
    df = pd.read_sql(CRM_EXTRACT_SQL, connection)
    
    # Сохраняем в XCom для передачи в следующую задачу
    context['ti'].xcom_push(key='crm_data', value=df.to_json(orient='records'))
    logging.info(f"Извлечено {len(df)} записей из CRM")
    return len(df)

def extract_telemetry(**context):
    """Извлечение данных телеметрии"""
    pg_hook = PostgresHook(postgres_conn_id='medical_db')
    connection = pg_hook.get_conn()
    
    df = pd.read_sql(TELEMETRY_EXTRACT_SQL, connection)
    
    context['ti'].xcom_push(key='telemetry_data', value=df.to_json(orient='records'))
    logging.info(f"Извлечено {len(df)} записей телеметрии")
    return len(df)

def transform_and_merge(**context):
    import json
    import logging
    import traceback
    from datetime import datetime
    
    logging.info("=" * 50)
    logging.info("НАЧАЛО ТРАНСФОРМАЦИИ")
    logging.info("=" * 50)
    
    try:
        # Получаем данные из XCom
        crm_json = context['ti'].xcom_pull(key='crm_data', task_ids='extract_crm')
        telemetry_json = context['ti'].xcom_pull(key='telemetry_data', task_ids='extract_telemetry')
        
        logging.info(f"CRM data: {type(crm_json)}, длина: {len(crm_json) if crm_json else 0}")
        logging.info(f"Telemetry data: {type(telemetry_json)}, длина: {len(telemetry_json) if telemetry_json else 0}")
        
        # Парсинг CRM
        if crm_json:
            df_crm = pd.DataFrame(json.loads(crm_json))
            logging.info(f"CRM columns: {df_crm.columns.tolist()}")
            df_crm['user_id'] = df_crm['user_id'].astype(str)
            if 'prosthesis_id' in df_crm.columns:
                df_crm['prosthesis_id'] = df_crm['prosthesis_id'].astype(str)
            else:
                df_crm['prosthesis_id'] = df_crm['user_id'] + '_prost'
                logging.warning("В CRM отсутствует prosthesis_id, создано искусственное значение")
        else:
            df_crm = pd.DataFrame()
            logging.warning("CRM данные пусты")
        
        # Парсинг телеметрии
        if telemetry_json:
            df_telemetry = pd.DataFrame(json.loads(telemetry_json))
            logging.info(f"Telemetry columns: {df_telemetry.columns.tolist()}")
            
            if 'user_id' in df_telemetry.columns:
                df_telemetry['user_id'] = df_telemetry['user_id'].astype(str)
            else:
                logging.error("В телеметрии отсутствует колонка user_id")
                raise ValueError("user_id not found in telemetry data")
            
            if 'prosthesis_id' in df_telemetry.columns:
                df_telemetry['prosthesis_id'] = df_telemetry['prosthesis_id'].astype(str)
            else:
                df_telemetry['prosthesis_id'] = df_telemetry['user_id'] + '_prost'
                logging.warning("В телеметрии отсутствует prosthesis_id, создано искусственное значение")
            
            # Корректное формирование report_date
            if 'timestamp' in df_telemetry.columns:
                # Если есть timestamp, используем его
                df_telemetry['report_date'] = pd.to_datetime(df_telemetry['timestamp']).dt.strftime('%Y-%m-%d')
                logging.info(f"Report_date из timestamp: {df_telemetry['report_date'].iloc[0] if not df_telemetry.empty else 'None'}")
            elif 'date' in df_telemetry.columns:
                df_telemetry['report_date'] = pd.to_datetime(df_telemetry['date']).dt.strftime('%Y-%m-%d')
                logging.info(f"Report_date из date: {df_telemetry['report_date'].iloc[0] if not df_telemetry.empty else 'None'}")
            else:
                # Используем сегодняшнюю дату
                today = datetime.now().strftime('%Y-%m-%d')
                df_telemetry['report_date'] = today
                logging.warning(f"Колонки 'timestamp' и 'date' отсутствуют, используется текущая дата: {today}")
        else:
            df_telemetry = pd.DataFrame()
            logging.warning("Телеметрия пуста")
        
        # Проверка наличия данных
        if df_telemetry.empty or df_crm.empty:
            logging.warning("Нет данных для объединения")
            df_merged = pd.DataFrame(columns=[
                'report_date', 'user_id', 'prosthesis_id',
                'user_full_name', 'user_email', 'prosthesis_model',
                'gesture_count', 'avg_response_time_ms', 
                'p95_response_time_ms', 'error_count', 
                'active_minutes', 'battery_health_score'
            ])
        else:
            df_merged = pd.merge(df_telemetry, df_crm, on='user_id', how='inner')
            logging.info(f"Merged shape: {df_merged.shape}")
            
            if df_merged.empty:
                logging.warning("Нет совпадающих записей для объединения")
                df_merged = pd.DataFrame(columns=[
                    'report_date', 'user_id', 'prosthesis_id',
                    'user_full_name', 'user_email', 'prosthesis_model',
                    'gesture_count', 'avg_response_time_ms', 
                    'p95_response_time_ms', 'error_count', 
                    'active_minutes', 'battery_health_score'
                ])
            else:
                rename_map = {
                    'full_name': 'user_full_name',
                    'email': 'user_email',
                    'model': 'prosthesis_model'
                }
                df_merged = df_merged.rename(columns=rename_map)
                
                # Убеждаемся, что report_date в правильном формате
                if 'report_date' in df_merged.columns:
                    # Проверяем, что дата валидная
                    try:
                        df_merged['report_date'] = pd.to_datetime(df_merged['report_date']).dt.strftime('%Y-%m-%d')
                    except Exception as e:
                        logging.warning(f"Ошибка преобразования даты: {e}, используется текущая дата")
                        df_merged['report_date'] = datetime.now().strftime('%Y-%m-%d')
                else:
                    df_merged['report_date'] = datetime.now().strftime('%Y-%m-%d')
                    logging.warning("report_date отсутствует, установлена текущая дата")
                
                # Убеждаемся, что prosthesis_id не пустой
                if 'prosthesis_id' in df_merged.columns:
                    df_merged['prosthesis_id'] = df_merged['prosthesis_id'].fillna(df_merged['user_id'] + '_prost')
                else:
                    df_merged['prosthesis_id'] = df_merged['user_id'] + '_prost'
                    logging.warning("prosthesis_id отсутствует, создано искусственное значение")
                
                expected_cols = [
                    'user_id', 
                    'prosthesis_id',
                    'user_full_name', 
                    'user_email', 
                    'prosthesis_model',
                    'gesture_count', 
                    'avg_response_time_ms', 
                    'p95_response_time_ms', 
                    'error_count', 
                    'active_minutes', 
                    'battery_health_score'
                ]
                
                available_cols = [col for col in expected_cols if col in df_merged.columns]
                missing_cols = [col for col in expected_cols if col not in df_merged.columns]
                if missing_cols:
                    logging.warning(f"Отсутствуют колонки: {missing_cols}")
                    for col in missing_cols:
                        df_merged[col] = None
                df_merged = df_merged[expected_cols]
        
        # Сохраняем в CSV
        csv_path = '/tmp/mart_data.csv'
        df_merged.to_csv(csv_path, index=False, header=True)
        logging.info(f"Сохранено {len(df_merged)} записей в {csv_path}")
        logging.info(f"Колонки в CSV: {df_merged.columns.tolist()}")
        if not df_merged.empty:
            logging.info(f"Пример данных: {df_merged.iloc[0].to_dict()}")
        
        context['ti'].xcom_push(key='merged_data_path', value=csv_path)
        logging.info("ТРАНСФОРМАЦИЯ ЗАВЕРШЕНА УСПЕШНО")
        return len(df_merged)
        
    except Exception as e:
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logging.error(traceback.format_exc())
        raise

def load_to_mart(**context):
    """Загрузка данных в витрину с обработкой дубликатов"""
    csv_path = context['ti'].xcom_pull(key='merged_data_path', task_ids='transform_and_merge')
    
    if not csv_path:
        logging.warning("Нет данных для загрузки")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='olap_db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    try:
        # 1. Создаём временную таблицу
        cursor.execute("""
            CREATE TEMP TABLE temp_mart_load (
                user_id VARCHAR(50),
                prosthesis_id VARCHAR(50),
                user_full_name VARCHAR(255),
                user_email VARCHAR(255),
                prosthesis_model VARCHAR(100),
                gesture_count INTEGER,
                avg_response_time_ms FLOAT,
                p95_response_time_ms FLOAT,
                error_count INTEGER,
                active_minutes INTEGER,
                battery_health_score FLOAT
            )
        """)
        
        # 2. Загружаем данные во временную таблицу через COPY
        with open(csv_path, 'r') as f:
            cursor.copy_expert(
                """
                COPY temp_mart_load 
                (user_id, prosthesis_id, user_full_name, user_email, 
                 prosthesis_model, gesture_count, avg_response_time_ms, 
                 p95_response_time_ms, error_count, active_minutes, battery_health_score)
                FROM STDIN WITH CSV HEADER
                """,
                f
            )
        
        # 3. Очищаем целевую таблицу (для тестов) или выполняем UPSERT
        # Вариант A: Очистить таблицу перед загрузкой (для тестов)
        # cursor.execute("TRUNCATE TABLE reports.mart_daily_user_report")
        
        # Вариант B: UPSERT с обработкой конфликтов
        cursor.execute("""
            INSERT INTO reports.mart_daily_user_report 
            (user_id, prosthesis_id, user_full_name, user_email, 
             prosthesis_model, gesture_count, avg_response_time_ms, 
             p95_response_time_ms, error_count, active_minutes, battery_health_score,
             report_generated_at)
            SELECT 
                user_id, prosthesis_id, user_full_name, user_email,
                prosthesis_model, gesture_count, avg_response_time_ms,
                p95_response_time_ms, error_count, active_minutes, battery_health_score,
                CURRENT_TIMESTAMP
            FROM temp_mart_load
            ON CONFLICT (report_generated_at, user_id, prosthesis_id) DO UPDATE SET
                user_full_name = EXCLUDED.user_full_name,
                user_email = EXCLUDED.user_email,
                prosthesis_model = EXCLUDED.prosthesis_model,
                gesture_count = EXCLUDED.gesture_count,
                avg_response_time_ms = EXCLUDED.avg_response_time_ms,
                p95_response_time_ms = EXCLUDED.p95_response_time_ms,
                error_count = EXCLUDED.error_count,
                active_minutes = EXCLUDED.active_minutes,
                battery_health_score = EXCLUDED.battery_health_score,
                report_generated_at = CURRENT_TIMESTAMP
        """)
        
        connection.commit()
        logging.info("Данные успешно загружены в витрину")
        
    except Exception as e:
        connection.rollback()
        logging.error(f"Ошибка загрузки: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

# Определение задач DAG

create_mart = PostgresOperator(
    task_id='create_mart_table',
    postgres_conn_id='olap_db',
    sql=CREATE_MART_SQL,
    dag=dag
)

extract_crm_task = PythonOperator(
    task_id='extract_crm',
    python_callable=extract_crm,
    provide_context=True,
    dag=dag
)

extract_telemetry_task = PythonOperator(
    task_id='extract_telemetry',
    python_callable=extract_telemetry,
    provide_context=True,
    dag=dag
)

transform_merge_task = PythonOperator(
    task_id='transform_and_merge',
    python_callable=transform_and_merge,
    provide_context=True,
    dag=dag
)

load_mart_task = PythonOperator(
    task_id='load_to_mart',
    python_callable=load_to_mart,
    provide_context=True,
    dag=dag
)

# Зависимости
create_mart >> [extract_crm_task, extract_telemetry_task] >> transform_merge_task >> load_mart_task