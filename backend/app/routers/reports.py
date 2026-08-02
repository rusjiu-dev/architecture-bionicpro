# backend/app/routers/reports.py

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from datetime import date, timedelta
from typing import Optional
import logging
import math

from app.database import get_db
from app.auth import get_current_user
from app.clickhouse_client import clickhouse_client

router = APIRouter(prefix="/api/v1", tags=["reports"])
logger = logging.getLogger(__name__)

@router.get("/reports")
async def get_user_report(
    user_id: str = Query(..., description="ID пользователя"),
    date_from: Optional[date] = Query(None, description="Начало периода"),
    date_to: Optional[date] = Query(None, description="Конец периода"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    # Проверка прав
    token_sub = current_user.get("sub")
    token_username = current_user.get("username")
    
    if user_id not in [token_sub, token_username]:
        logger.warning(f"User {token_username} ({token_sub}) attempted to access report for {user_id}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not authorized to view this report"
        )
    
    # Определение периода
    if not date_from:
        date_from = date.today() - timedelta(days=30)
    if not date_to:
        date_to = date.today()
    
    try:
        logger.info(f"Fetching report for user: {user_id}, period: {date_from} - {date_to}")
        
        records = clickhouse_client.get_report(
            user_id=user_id,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat()
        )
        
        logger.info(f"Found {len(records)} records for user {user_id}")
        
        # Дополнительная очистка данных перед ответом
        cleaned_records = []
        for record in records:
            cleaned = {}
            for key, value in record.items():
                if isinstance(value, float):
                    if math.isnan(value) or math.isinf(value):
                        cleaned[key] = None
                    else:
                        cleaned[key] = value
                else:
                    cleaned[key] = value
            cleaned_records.append(cleaned)
        
        return {
            "status": "success" if cleaned_records else "not_found",
            "user_id": user_id,
            "username": current_user.get("username"),
            "period": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat()
            },
            "records": cleaned_records,
            "total_records": len(cleaned_records)
        }
        
    except Exception as e:
        logger.error(f"Error fetching report: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch report: {str(e)}"
        )