from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date, timedelta
from typing import Optional
import logging

from app.database import get_db
from app.auth import get_current_user

router = APIRouter(prefix="/api/v1", tags=["reports"])
logger = logging.getLogger(__name__)

@router.get("/reports")
async def get_user_report(
    user_id: str = Query(..., description="ID пользователя (username или UUID)"),
    date_from: Optional[date] = Query(None, description="Начало периода"),
    date_to: Optional[date] = Query(None, description="Конец периода"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Получение отчёта пользователя.
    user_id может быть как username (prothetic1), так и UUID из Keycloak.
    """
    # Проверка прав: пользователь может видеть только свои данные
    token_sub = current_user.get("sub")
    token_username = current_user.get("username")
    
    # Разрешаем доступ, если user_id совпадает с sub ИЛИ с username
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
        # Запрашиваем данные из витрины
        # user_id может быть username (prothetic1) или UUID
        sql_query = text("""
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
            FROM reports.mart_daily_user_report
            WHERE user_id = :user_id
              AND DATE(report_generated_at) BETWEEN :date_from AND :date_to
            ORDER BY report_generated_at DESC
        """)
        
        result = db.execute(sql_query, {
            "user_id": user_id,
            "date_from": date_from,
            "date_to": date_to
        })
        
        report_data = result.mappings().all()
        
        return {
            "status": "success" if report_data else "not_found",
            "user_id": user_id,
            "username": current_user.get("username"),
            "period": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat()
            },
            "records": [dict(row) for row in report_data],
            "total_records": len(report_data)
        }
        
    except Exception as e:
        logger.error(f"Error fetching report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch report: {str(e)}"
        )