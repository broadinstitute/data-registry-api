import hashlib
from uuid import UUID

ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
BASE58 = {char: index for index, char in enumerate(ALPHABET)}
BASE = 58


def base58_encode(num):
    """Encode a number using Base58."""
    num = int(num.hex(), 16)
    encoding = ""
    while num:
        num, remainder = divmod(num, BASE)
        encoding = ALPHABET[remainder] + encoding
    return encoding


def shorten_uuid(u_hex):
    """Convert UUID to shortened version."""
    u = UUID(u_hex)
    sha256 = hashlib.sha256()
    sha256.update(u.bytes)
    hashed = sha256.digest()
    return base58_encode(hashed)[:6]  # Adjust the slice
