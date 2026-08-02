# backend/app/clickhouse_client.py

from clickhouse_driver import Client
import os
import logging
from datetime import date, datetime
from decimal import Decimal
import math

logger = logging.getLogger(__name__)

class ClickHouseClient:
    def __init__(self):
        self.host = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").replace("http://", "").replace(":8123", "")
        self.port = 9000
        self.database = os.getenv("CLICKHOUSE_DB", "reports")
        self.user = os.getenv("CLICKHOUSE_USER", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                self._client = Client(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    secure=False,
                    connect_timeout=10,
                    send_receive_timeout=30
                )
                self._client.execute("SELECT 1")
                logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                raise
        return self._client

    def execute(self, query, params=None):
        try:
            if params:
                return self.client.execute(query, params)
            return self.client.execute(query)
        except Exception as e:
            logger.error(f"ClickHouse query error: {e}")
            logger.error(f"Query: {query}")
            raise

    def _serialize_value(self, value):
        """Преобразование значений для JSON-сериализации"""
        if value is None:
            return None
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bytes):
            return value.decode('utf-8')
        if isinstance(value, float):
            # Обработка inf и nan
            if math.isnan(value):
                return None
            if math.isinf(value):
                return None
            return value
        return value

    def _serialize_row(self, row):
        """Преобразование строки для JSON"""
        return [self._serialize_value(v) for v in row]

    def get_report(self, user_id: str, date_from: str, date_to: str):
        """Получение отчёта для пользователя"""
        query = """
            SELECT
                user_id,
                prosthesis_id,
                user_full_name,
                user_email,
                prosthesis_model,
                gesture_count,
                avg_response_time_ms,
                p95_response_time_ms,
                error_count,
                error_rate,
                battery_health_score,
                active_minutes,
                report_generated_at
            FROM mart_daily_user_report
            WHERE user_id = %(user_id)s
              AND DATE(report_generated_at) BETWEEN %(date_from)s AND %(date_to)s
            ORDER BY report_generated_at DESC
        """
        
        params = {
            'user_id': user_id,
            'date_from': date_from,
            'date_to': date_to
        }
        
        try:
            result = self.execute(query, params)
            columns = [
                'user_id', 'prosthesis_id', 'user_full_name', 'user_email',
                'prosthesis_model', 'gesture_count', 'avg_response_time_ms',
                'p95_response_time_ms', 'error_count', 'error_rate',
                'battery_health_score', 'active_minutes', 'report_generated_at'
            ]
            
            records = []
            for row in result:
                record = {}
                for idx, col in enumerate(columns):
                    record[col] = self._serialize_value(row[idx])
                records.append(record)
            
            return records
        except Exception as e:
            logger.error(f"Error fetching report for user {user_id}: {e}")
            raise

    def insert_report(self, data: dict):
        query = """
            INSERT INTO mart_daily_user_report (
                user_id, prosthesis_id, user_full_name, user_email,
                prosthesis_model, gesture_count, avg_response_time_ms,
                p95_response_time_ms, error_count, error_rate,
                battery_health_score, active_minutes, report_generated_at
            ) VALUES
        """
        values = [(
            data['user_id'],
            data['prosthesis_id'],
            data.get('user_full_name', ''),
            data.get('user_email', ''),
            data.get('prosthesis_model', ''),
            data.get('gesture_count', 0),
            data.get('avg_response_time_ms', 0.0),
            data.get('p95_response_time_ms', 0.0),
            data.get('error_count', 0),
            data.get('error_rate', 0.0),
            data.get('battery_health_score', 0.0),
            data.get('active_minutes', 0),
            data.get('report_generated_at', 'now()')
        )]
        
        try:
            self.execute(query, values)
            logger.info(f"Inserted report for user {data['user_id']}")
        except Exception as e:
            logger.error(f"Error inserting report: {e}")
            raise

    def health_check(self):
        try:
            result = self.execute("SELECT 1")
            return result and result[0][0] == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

# Глобальный экземпляр
clickhouse_client = ClickHouseClient()