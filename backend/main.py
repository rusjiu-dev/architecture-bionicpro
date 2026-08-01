from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.routers import reports

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="BionicPRO Reports API",
    version="1.0.0",
    description="API для получения отчётов пользователей из OLAP-витрины"
)

# CORS для фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",   # React dev server
        "http://localhost:8080",   # Keycloak (для обращений)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(reports.router)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "reports-api"}

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