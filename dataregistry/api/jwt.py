import os

from jose import jwt, JWTError

from dataregistry.api.config import get_sensitive_config
from dataregistry.api.model import User

SECRET_KEY = os.getenv('JWT_SECRET', get_sensitive_config()['jwtSecret'] if get_sensitive_config() else 'test_secret')
ALGORITHM = "HS256"


def get_encoded_cookie_data(user: User):
    return jwt.encode(user.dict(), SECRET_KEY, algorithm=ALGORITHM)


def get_decoded_cookie_data(cookie_data: str) -> dict:
    try:
        return jwt.decode(cookie_data, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None
