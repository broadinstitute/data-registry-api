import logging

import fastapi
import httpx

logger = logging.getLogger(__name__)

SESSION_EXPIRED_DETAIL = 'Your session has expired or is invalid. Please sign in again.'


def auth_failure_exception(response: httpx.Response) -> fastapi.HTTPException:
    """Translate a failed user-service verify response into an HTTPException.

    Always 401 to the client so callers treat it uniformly as a sign-in
    problem, but carry the user service's actual reason (group mismatch,
    non-membership) instead of a bare 'Invalid token'.
    """
    if response.status_code == 401:
        detail = SESSION_EXPIRED_DETAIL
    else:
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = body.get('error') or body.get('detail') or 'Invalid token'
            else:
                detail = 'Invalid token'
        except ValueError:
            detail = 'Invalid token'
    logger.warning('User service verify rejected: status=%s detail=%s',
                   response.status_code, detail)
    return fastapi.HTTPException(status_code=401, detail=detail)
