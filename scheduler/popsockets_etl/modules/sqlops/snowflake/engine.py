from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from popsockets_etl.modules.exceptions import SQLEngineException
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials, SFCredentialsKeyFile
from typing import Union
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64

class Engine:
    def __init__(self,credentials:Union[SFCredentials,SFCredentialsKeyFile]) -> None:
        self.credentials=credentials

    def get_engine(self):
        if isinstance(self.credentials, SFCredentials):
            engine_url = URL(
                            account = self.credentials.account,
                            user = self.credentials.user,
                            password = self.credentials.password,
                            database = self.credentials.database,
                            schema = self.credentials.schema,
                            warehouse = self.credentials.warehouse,
                            role=self.credentials.role
                            )
            engine = create_engine(engine_url)
            print("Snowflake SQLalchemy Engine Initiated...",flush=True)
            return engine

        elif isinstance(self.credentials, SFCredentialsKeyFile):
            return self.get_engine_from_key_file(self.credentials)
    
    def execute_query(self,statement,select:bool=False,insert:bool=False,update:bool=False,delete:bool=False,ddl=False,truncate=False,print_query=False):
        engine = self.get_engine()
        if print_query is True:
            print(f"Executing Query: {statement}...",flush=True)
        if insert is True or update is True or delete is True or ddl is True or truncate is True:
            with engine.begin() as conn:
                conn.execute(statement)
            query_type = None
            
            if insert is True:
                query_type = 'Insert'
            elif update is True:
                query_type = 'Update'
            elif delete is True:
                query_type = 'Delete'
            elif ddl is True:
                query_type = 'DDL'
            elif truncate is True:
                query_type = 'Truncate'

            print(f"{query_type} Query Execution Done...",flush=True)
            return None
        
        elif select is True:
            with engine.begin() as conn:
                response = conn.execute(statement)
                rows = response.fetchall()
                keys = response.keys()
                result_list = [dict(zip(keys, row)) for row in rows]
                print("Select Query Execution Done...",flush=True)
                return result_list
        else:
            raise SQLEngineException("Please set anyone of these parameteres as True:\n-select\n-insert\n-update")
        
    def get_engine_from_key_file(self,credentials:SFCredentialsKeyFile):
        with open(credentials.private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # If encrypted: use password=b"your_passphrase"
                backend=default_backend()
            )

        # Convert to DER (binary) and then base64 encode
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Base64 encode and decode to UTF-8 string
        private_key_b64 = base64.b64encode(private_key_der).decode("utf-8")
        engine_url = URL(
                        account=credentials.account,
                        user=credentials.user,
                        private_key=private_key_b64,
                        role=credentials.role,
                        warehouse=credentials.warehouse,
                        database=credentials.database,
                        schema=credentials.schema
                    )
        engine = create_engine(engine_url)
        print("Snowflake SQLalchemy Engine Initiated with Key File...",flush=True)
        return engine