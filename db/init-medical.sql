-- Создание схемы
CREATE SCHEMA IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.raw_events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    prosthesis_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    response_time_ms INTEGER,
    is_error BOOLEAN DEFAULT FALSE,
    battery_level SMALLINT CHECK (battery_level BETWEEN 0 AND 100),
    active_seconds INTEGER DEFAULT 0
);

-- Создание индексов для быстрых выборок
CREATE INDEX IF NOT EXISTS idx_telemetry_user_date ON telemetry.raw_events (user_id, DATE(timestamp));
CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry.raw_events (timestamp);

-- Загрузка тестовых данных
COPY telemetry.raw_events (user_id, prosthesis_id, timestamp, response_time_ms, is_error, battery_level, active_seconds)
FROM '/docker-entrypoint-initdb.d/telemetry_sample.csv'
DELIMITER ','
CSV HEADER;




