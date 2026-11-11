"""
Encrypts the bots' API passwords before they're saved.

WHY encrypt and not hash: Django HASHES user passwords (you can check a password,
never read it back). A bot must SEND its password to the plate API, so we need to
get the real value back. Encryption is reversible with the key; hashing isn't.

The key is made from Django's SECRET_KEY. If you change SECRET_KEY, stored bot
passwords can't be read anymore and you'd need to re-enter them.
"""
import base64
import hashlib

from cryptography.fernet import Fernet
from django.conf import settings


def _fernet() -> Fernet:
    key = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def encrypt(text: str) -> str:
    return _fernet().encrypt(text.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()
