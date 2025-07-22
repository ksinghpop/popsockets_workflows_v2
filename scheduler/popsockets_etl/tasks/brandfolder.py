import os
from dotenv import load_dotenv
from popsockets_etl.pipelines.brandfolder.brandfolder_pipelines import *
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials, SFCredentialsKeyFile
from popsockets_etl.modules.apis.brandfolder.apiv4 import BrandfolderApiConfig, BrandfolderURIs, BrandfolderSearh, BrandfolderParams
load_dotenv()


def get_credentials(warehouse,database,schema):
    credentials = SFCredentials(
                            account=os.environ.get('SF_ACCOUNT'),
                            warehouse=warehouse,
                            database=database,
                            schema=schema,
                            user=os.environ.get('SF_USER_2'),
                            password=os.environ.get('SF_USER_PSWD_2'),
                            role=os.environ.get('SF_ROLE_2')
                        )
    return credentials

def get_credentials_key_file(warehouse,database,schema):
    credentials = SFCredentialsKeyFile(
                            account=os.environ.get('SF_ACCOUNT'),
                            warehouse=warehouse,
                            database=database,
                            schema=schema,
                            user=os.environ.get('SF_USER_2'),
                            private_key_path=os.environ.get('SF_RSA_PRIVATE_KEY_PATH'),
                            role=os.environ.get('SF_ROLE_2')
                        )
    return credentials

common_data_sync_hours = 26
PRODUCTION_DB='PROD'
PRODUCTION_SCHEMA='BRANDFOLDER'
BRANDFOLDER_API_KEY=os.environ.get('BRANDFOLDER_API_KEY')


def organisation_task():
    uri = BrandfolderURIs.get_organisations()
    params = BrandfolderParams(per=100,fields='created_at,updated_at')
    api_config = BrandfolderApiConfig(uri=uri,params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file(warehouse="DEV_ANALYSTS_XS",database=PRODUCTION_DB,schema=PRODUCTION_SCHEMA)
    print(sf_credentials,flush=True)
    response = organisation(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def brandfolder_task():
    uri = BrandfolderURIs.get_brandfolders()
    params = BrandfolderParams(per=100,fields='created_at,updated_at')
    api_config = BrandfolderApiConfig(uri=uri,params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = brandfolder(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def section_task():
    params = BrandfolderParams(per=100,fields='created_at,updated_at')
    api_config = BrandfolderApiConfig(params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = section(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def collection_task():
    params = BrandfolderParams(per=100,fields='created_at,updated_at')
    api_config = BrandfolderApiConfig(params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = collection(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def asset_task():
    search_query = BrandfolderSearh.last_n_hours(common_data_sync_hours)
    params = BrandfolderParams(per=3000,fields='cdn_url,created_at,updated_at,section,collections',include='custom_fields,tags',search=search_query)
    api_config = BrandfolderApiConfig(params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = asset(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def attachment_task():
    search_query = BrandfolderSearh.last_n_hours(common_data_sync_hours)
    params = BrandfolderParams(per=3000,fields='cdn_url,created_at,updated_at,asset',search=search_query)
    api_config = BrandfolderApiConfig(params=params,api_key=BRANDFOLDER_API_KEY)
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = attachment(brandflder_api_config=api_config,snowflake_credentials=sf_credentials)
    return response
    # return None


def asset_custom_values_task():
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = asset_custom_fields(snowflake_credentials=sf_credentials,select_table_last_hours=common_data_sync_hours)
    return response
    # return None


def asset_collections_task():
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = asset_collections(snowflake_credentials=sf_credentials,select_table_last_hours=common_data_sync_hours)
    return response
    # return None


def brandfolder_sku_urls_task():
    sf_credentials = get_credentials_key_file("DEV_ANALYSTS_XS",PRODUCTION_DB,PRODUCTION_SCHEMA)
    response = brandfolder_sku_urls(sf_credentials)
    return response