from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import json
import base64

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    
    try:
        # Декодируем токен без проверки подписи
        # Разбиваем токен на части: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token format"
            )
        
        # Декодируем payload (вторая часть)
        payload_part = parts[1]
        # Добавляем padding если нужно
        payload_part += '=' * (4 - len(payload_part) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_part)
        payload = json.loads(payload_bytes)
        
        # Извлекаем информацию о пользователе
        user_id = payload.get("sub")
        username = payload.get("preferred_username")
        roles = payload.get("realm_access", {}).get("roles", [])
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing subject"
            )
        
        return {
            "sub": user_id,
            "username": username,
            "roles": roles
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}"
        )