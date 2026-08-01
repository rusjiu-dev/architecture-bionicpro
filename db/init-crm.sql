CREATE SCHEMA IF NOT EXISTS crm;

CREATE TABLE IF NOT EXISTS crm.users (
    id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    registration_date DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS crm.prostheses (
    id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES crm.users(id),
    model VARCHAR(100) NOT NULL,
    purchase_date DATE NOT NULL,
    warranty_end_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Загрузка данных из отдельных файлов
COPY crm.users (id, full_name, email, phone, registration_date, is_active)
FROM '/docker-entrypoint-initdb.d/crm_users.csv'
DELIMITER ','
CSV HEADER;

COPY crm.prostheses (id, user_id, model, purchase_date, warranty_end_date, is_active)
FROM '/docker-entrypoint-initdb.d/crm_prostheses.csv'
DELIMITER ','
CSV HEADER;