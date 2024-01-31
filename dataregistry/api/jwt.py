import os
from datetime import datetime, timedelta

from jose import jwt, JWTError

from dataregistry.api.config import get_sensitive_config
from dataregistry.api.model import User

SECRET_KEY = os.getenv('JWT_SECRET', get_sensitive_config()['jwtSecret'] if get_sensitive_config() else 'test_secret')
ALGORITHM = "HS256"


def get_encoded_jwt_data(user: User, expires_delta: timedelta = timedelta(days=10)):
    to_encode = user.dict()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_decoded_jwt_data(cookie_data: str) -> dict:
    try:
        return jwt.decode(cookie_data, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None
