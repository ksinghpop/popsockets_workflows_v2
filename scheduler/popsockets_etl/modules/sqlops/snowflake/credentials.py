from dataclasses import dataclass
from typing import Optional

@dataclass
class SFCredentials:
    """This object will bear all credentials required to create sqlalchemy engine"""
    account:str
    warehouse:str
    database:str
    schema:str
    user:str
    password:str
    role:str

@dataclass
class SFCredentialsKeyFile:
    """This object will bear all credentials required to create sqlalchemy engine from a key file"""
    account:str
    warehouse:str
    database:str
    schema:str
    user:str
    private_key_path:str
    role:str