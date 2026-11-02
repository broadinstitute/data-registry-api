"""Public-facing CalR experiment identifiers.

A submission's primary key stays an opaque 32-char uuid hex (it is a binary(32)
column and is embedded in S3 object keys). The CalR id is a second, shorter
identifier that users actually see and paste: it appears in share links, in
/analysis/<calr_id> URLs, and in downloaded file names.

The alphabet is base58 — 0/O/I/l are omitted — so an id read off a screen or
dictated over a phone has no ambiguous characters. Mixed case is deliberate: it
buys enough entropy to stay short at 8 characters (58^8, ~1.3e14).
"""
import secrets

from dataregistry.id_shortener import ALPHABET

CALR_ID_PREFIX = 'CalR'
CALR_ID_LENGTH = 8


def generate_calr_id() -> str:
    """Generate a random public-facing CalR id, e.g. CalR7bQk2Rfp.

    Uniqueness is enforced by the UNIQUE constraint on calr_submissions.calr_id,
    not by this function; callers insert with retry (see insert_calr_submission).
    """
    suffix = ''.join(secrets.choice(ALPHABET) for _ in range(CALR_ID_LENGTH))
    return f"{CALR_ID_PREFIX}{suffix}"
