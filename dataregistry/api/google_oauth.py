import os
import requests


CLIENT_ID = os.getenv('GOOGLE_OAUTH_CLIENT_ID')
CLIENT_SECRET = os.getenv('GOOGLE_OAUTH_CLIENT_SECRET')
REDIRECT_URI = os.getenv('GOOGLE_OAUTH_REDIRECT_URI')


def get_google_user(code: str):
    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    token_response = requests.post(token_url, data=token_data)

    user_info_url = "https://www.googleapis.com/oauth2/v2/userinfo"
    access_token = token_response.json()["access_token"]
    user_info_response = requests.get(user_info_url, params={"access_token": access_token})
    user_info = user_info_response.json()

    return user_info
