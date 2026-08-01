from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv
import logging

logger = logging.getLogger(__name__)

# Аргументы по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

def insert_data_from_csv():
    """
    Читает CSV-файл и напрямую вставляет данные в PostgreSQL
    через параметризованные запросы
    """
    CSV_FILE_PATH = '/opt/airflow/sample_files/sample.csv'
    TABLE_NAME = 'sample_table'
    
    hook = PostgresHook(postgres_conn_id='write_to_postgres')
    
    try:
        with open(CSV_FILE_PATH, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            
            # Пропускаем заголовок
            header = next(csvreader)
            logger.info(f"Заголовки CSV: {header}")
            
            # Счётчики для логирования
            total_rows = 0
            success_rows = 0
            error_rows = 0
            
            # Получаем соединение и создаём курсор
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    for row_num, row in enumerate(csvreader, start=2):  # start=2 т.к. 1-я строка — заголовок
                        try:
                            # Проверяем, что в строке достаточно колонок
                            if len(row) < 5:
                                logger.warning(f"Строка {row_num}: недостаточно колонок, пропускаем")
                                error_rows += 1
                                continue
                            
                            # Параметризованный запрос — защита от SQL-инъекций
                            cur.execute(
                                """
                                INSERT INTO sample_table 
                                (id, order_number, total, discount, buyer_id) 
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (
                                    row[0].strip() if row[0].strip() else None,
                                    row[1].strip() if row[1].strip() else None,
                                    row[2].strip() if row[2].strip() else None,
                                    row[3].strip() if row[3].strip() else None,
                                    row[4].strip() if row[4].strip() else None,
                                )
                            )
                            success_rows += 1
                            
                        except Exception as e:
                            logger.error(f"Ошибка в строке {row_num}: {row}. Ошибка: {e}")
                            error_rows += 1
                            # Продолжаем обработку остальных строк
                            continue
                    
                    # Коммитим транзакцию
                    conn.commit()
                    
        # Итоговая статистика
        logger.info(f"Загрузка завершена. Всего строк: {total_rows}, "
                   f"успешно: {success_rows}, ошибок: {error_rows}")
        
        if error_rows > 0:
            logger.warning(f"Были пропущены строки с ошибками: {error_rows}")
            
    except FileNotFoundError:
        logger.error(f"Файл {CSV_FILE_PATH} не найден!")
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка при загрузке данных: {e}")
        raise

# Определяем DAG
with DAG(
    'csv_to_postgres_dag_v2',
    default_args=default_args,
    description='Загрузка CSV в PostgreSQL через PostgresHook',
    schedule_interval='@once',
    catchup=False,
    tags=['postgres', 'csv', 'etl'],
) as dag:

    # Создаём таблицу в PostgreSQL
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='write_to_postgres',
        sql="""
        DROP TABLE IF EXISTS sample_table;
        CREATE TABLE sample_table (
            id SERIAL PRIMARY KEY,
            order_number BIGINT,
            total NUMERIC(18,2),
            discount NUMERIC(18,2),
            buyer_id BIGINT
        );
        """
    )

    # Вставляем данные напрямую через PythonOperator
    insert_data = PythonOperator(
        task_id='insert_data_from_csv',
        python_callable=insert_data_from_csv,
    )

    # Порядок выполнения
    create_table >> insert_data