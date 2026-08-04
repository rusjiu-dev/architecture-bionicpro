-- clickhouse/init.sql

-- Создание базы данных
CREATE DATABASE IF NOT EXISTS reports;

-- Создание таблицы витрины
CREATE TABLE IF NOT EXISTS reports.mart_daily_user_report (
    user_id String,
    prosthesis_id String,
    user_full_name String,
    user_email String,
    prosthesis_model String,
    gesture_count UInt32,
    avg_response_time_ms Float64,
    p95_response_time_ms Float64,
    error_count UInt32,
    error_rate Float64,
    battery_health_score Float64,
    active_minutes UInt32,
    report_generated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, report_generated_at)
PARTITION BY toYYYYMM(report_generated_at);

-- Создание индексов
ALTER TABLE reports.mart_daily_user_report ADD INDEX idx_user_id user_id TYPE bloom_filter GRANULARITY 1;