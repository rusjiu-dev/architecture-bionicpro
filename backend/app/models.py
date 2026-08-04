from sqlalchemy import Column, String, Integer, Float, TIMESTAMP, text
from app.database import Base

class MartDailyUserReport(Base):
    __tablename__ = "mart_daily_user_report"
    __table_args__ = {"schema": "reports"}

    user_id = Column(String(50), primary_key=True)
    prosthesis_id = Column(String(50), primary_key=True)
    report_generated_at = Column(TIMESTAMP, primary_key=True, server_default=text("CURRENT_TIMESTAMP"))
    user_full_name = Column(String(255))
    user_email = Column(String(255))
    prosthesis_model = Column(String(100))
    gesture_count = Column(Integer, default=0)
    avg_response_time_ms = Column(Float)
    p95_response_time_ms = Column(Float)
    error_count = Column(Integer, default=0)
    error_rate = Column(Float)
    battery_health_score = Column(Float)
    active_minutes = Column(Integer, default=0)