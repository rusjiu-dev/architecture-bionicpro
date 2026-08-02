# backend/app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.routers import reports
from app.clickhouse_client import clickhouse_client

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="BionicPRO Reports API",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(reports.router)

@app.get("/health")
async def health_check():
    """Проверка здоровья всех сервисов"""
    ch_healthy = clickhouse_client.health_check()
    return {
        "status": "healthy" if ch_healthy else "unhealthy",
        "clickhouse": ch_healthy
    }

@app.get("/")
async def root():
    return {
        "service": "BionicPRO Reports API",
        "version": "1.0.0",
        "endpoints": {
            "/health": "GET - Health check",
            "/api/v1/reports": "GET - Get user report (requires auth)"
        }
    }