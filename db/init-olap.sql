-- Создание схемы для отчётов
CREATE SCHEMA IF NOT EXISTS reports;

CREATE TABLE IF NOT EXISTS reports.mart_daily_user_report (
    user_id VARCHAR(50) NOT NULL,
    prosthesis_id VARCHAR(50) NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_mart_user ON reports.mart_daily_user_report (user_id);
CREATE INDEX IF NOT EXISTS idx_mart_generated_at ON reports.mart_daily_user_report (report_generated_at DESC);