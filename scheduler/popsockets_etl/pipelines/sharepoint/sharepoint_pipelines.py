from popsockets_etl.modules.apis.sharepoint.api import *
from popsockets_etl.modules.transformation.sharepoint.transform import *
from popsockets_etl.modules.transformation.sharepoint.mapping import *
from popsockets_etl.modules.transformation.asin_data_app.mapping import Map as asin_map
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, DeleteQuery, SelectQuery, TruncateTableQuery, CustomQuery
import datetime as dt
import pandas as pd
import numpy as np

class SharepointPipeline:
    
    @staticmethod
    def generic(
            snowflake_credentials:SFCredentials,
            snowflake_tablename:str,
            azure_app_credentials:AzureAppCredentials,
            sharepoint_config:SharepointConfig,
            microsoft_graph_config:MicrosoftGraphApiConfig,
            file_type:str='xlsx',
            # pipeline_name:str
            ):
        print("Logging in to Microsoft Services...",flush=True)
        microsoft_login_obj = MicrosoftAzureAppDelegatedLogin(
                                        tenant_id=azure_app_credentials.tenant_id,
                                        grant_type=azure_app_credentials.grant_type,
                                        client_id=azure_app_credentials.client_id,
                                        client_secret_value=azure_app_credentials.client_secret_value,
                                        scope=microsoft_graph_config.scope
                                    )
        print("Retrieving Access Token...",flush=True)
        access_token = microsoft_login_obj.generate_token()

        print("Initiating Sharepoint Drive Object...",flush=True)
        drive_obj = DriveAPI(
                            host=microsoft_graph_config.host,
                            drive_id=sharepoint_config.drive_id,
                            root_to_target_folder_path=sharepoint_config.path,
                            access_token=access_token
                        )
        print(f"Getting all files metadata with following sharepoint path and filters:\nPath:{sharepoint_config.path}\nFilter:{microsoft_graph_config.filter}",flush=True)
        json_data = drive_obj.get_content_by_folder_path(filter=microsoft_graph_config.filter,days=microsoft_graph_config.days)
        sql_engine = Engine(snowflake_credentials)
        sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)


        print("Processing files...")
        for record in json_data:
            print("=========================================================================================",flush=True)
            file_id = record['id']
            file_name = record['name']
            parent_path = record['parentReference']['path']
            print(f"Ingesting data for following file:\nFile Id:{file_id}\nFilaName:{file_name}\nFilePath:{parent_path}")

            print("Getting binary data of the file...",flush=True)
            file_bytes_data = drive_obj.get_data(sharepoint_path=parent_path,filename=file_name)
            serialized_dict = None
            
            if isinstance(file_bytes_data,bytes) is not True:
                print(f"Not able to fetch data from Microsoft Sharepoint Graph API. Getting following error:\n")
                print(file_bytes_data)
                break
            else:
                print("Converting binary data into Dataframe...",flush=True)
                df:pd.DataFrame = bytes_to_pandas_df(bytes_data=file_bytes_data,file_type=file_type)
                df.replace(np.nan,None,inplace=True)
                print("Adding additional pipeline default fields...",flush=True)
                df['sharepoint_file_id'] = file_id
                df['sharepoint_file_name'] = file_name
                # df['pipeline_name'] = pipeline_name
                df['pipeline_data_sync_utc_at'] = (dt.datetime.now(dt.timezone.utc)).strftime('%Y-%m-%d %H:%M:%SZ')

                print("Converting final Dataframe into list of dicts...",flush=True)
                serialized_dict = df.to_dict('records')
            
            if serialized_dict is not None and len(serialized_dict) > 0:
                final_dict = []
                
                print("Field Mapping in progress...",flush=True)
                for record in serialized_dict:
                    final_dict.append(getattr(Map,snowflake_tablename)(record))

                print("Deleting records in table by sharepoint file id if already exists...",flush=True)
                id_delete_query = DeleteQuery(sql_table).delete_by_id(id_column_name='sharepoint_file_id',id_value=file_id)
                file_name_delete_query = CustomQuery.get_query(f"DELETE FROM {snowflake_tablename} WHERE sharepoint_file_name = '{file_name}'")
                sql_engine.execute_query(statement=id_delete_query,delete=True)
                sql_engine.execute_query(statement=file_name_delete_query,delete=True)
                print("Inserting records...",flush=True)
                insert_query = InsertQuery(sql_table).insert_all(final_dict)
                sql_engine.execute_query(statement=insert_query,insert=True)
                print("Inserting records...",flush=True)
                print(f"Records Inserted:{len(final_dict)}",flush=True)
                print("=========================================================================================",flush=True)
            else:
                print(f"No Data Found in file: {file_name}")
                print("=========================================================================================",flush=True)

    @staticmethod
    def contentup(
            snowflake_credentials:SFCredentials,
            snowflake_tablename:str,
            mapping_object_name:str,
            azure_app_credentials:AzureAppCredentials,
            sharepoint_config:SharepointConfig,
            microsoft_graph_config:MicrosoftGraphApiConfig,
            # pipeline_name:str
            ):
        print("Logging in to Microsoft Services...",flush=True)
        microsoft_login_obj = MicrosoftAzureAppDelegatedLogin(
                                        tenant_id=azure_app_credentials.tenant_id,
                                        grant_type=azure_app_credentials.grant_type,
                                        client_id=azure_app_credentials.client_id,
                                        client_secret_value=azure_app_credentials.client_secret_value,
                                        scope=microsoft_graph_config.scope
                                    )
        print("Retrieving Access Token...",flush=True)
        access_token = microsoft_login_obj.generate_token()

        print("Initiating Sharepoint Drive Object...",flush=True)
        drive_obj = DriveAPI(
                            host=microsoft_graph_config.host,
                            drive_id=sharepoint_config.drive_id,
                            root_to_target_folder_path=sharepoint_config.path,
                            access_token=access_token
                        )
        print(f"Getting all files metadata with following sharepoint path and filters:\nPath:{sharepoint_config.path}\nFilter:{microsoft_graph_config.filter}",flush=True)
        json_data = drive_obj.get_content_by_folder_path(filter=microsoft_graph_config.filter,days=microsoft_graph_config.days)
        sql_engine = Engine(snowflake_credentials)
        sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)


        print("Processing files...")
        for record in json_data:
            print("=========================================================================================",flush=True)
            file_id = record['id']
            file_name = record['name']
            parent_path = record['parentReference']['path']
            print(f"Ingesting data for following file:\nFile Id:{file_id}\nFilaName:{file_name}\nFilePath:{parent_path}")

            print("Getting binary data of the file...",flush=True)
            file_bytes_data = drive_obj.get_data(sharepoint_path=parent_path,filename=file_name)
            serialized_dict = None
            
            if isinstance(file_bytes_data,bytes) is not True:
                print(f"Not able to fetch data from Microsoft Sharepoint Graph API. Getting following error:\n")
                print(file_bytes_data)
                break
            else:
                print("Converting binary data into Dataframe...",flush=True)
                df:pd.DataFrame = bytes_to_pandas_df(file_bytes_data)
                df.replace(np.nan,None,inplace=True)
                print("Adding additional pipeline default fields...",flush=True)
                df['updated_at'] = (dt.datetime.now(dt.timezone.utc)).strftime('%Y-%m-%d %H:%M:%SZ')

                print("Converting final Dataframe into list of dicts...",flush=True)
                serialized_dict = df.to_dict('records')
            
            if serialized_dict is not None and len(serialized_dict) > 0:
                final_dict = []
                
                print("Field Mapping in progress...",flush=True)
                for record in serialized_dict:
                    final_dict.append(getattr(asin_map,mapping_object_name)(record))

                print("Deleting records in table by sharepoint file id if already exists...",flush=True)
                truncate_query = TruncateTableQuery(sql_table).truncate()
                sql_engine.execute_query(statement=truncate_query,truncate=True)
                print("Inserting records...",flush=True)
                insert_query = InsertQuery(sql_table).insert_all(final_dict)
                sql_engine.execute_query(statement=insert_query,insert=True)
                print("Inserting records...",flush=True)
                print(f"Records Inserted:{len(final_dict)}",flush=True)
                print("=========================================================================================",flush=True)
            else:
                print(f"No Data Found in file: {file_name}")
                print("=========================================================================================",flush=True)



class ShopifyDailyInventoryUpdate:

    @staticmethod
    def upload_csv_file(
                        snowflake_credentials:SFCredentials,
                        snowflake_tablename:str,
                        azure_app_credentials:AzureAppCredentials,
                        sharepoint_config:SharepointConfig,
                        microsoft_graph_config:MicrosoftGraphApiConfig
                        ):
        
        sql_engine = Engine(snowflake_credentials)
        sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
        select_query = SelectQuery(sql_table).select()
        data = sql_engine.execute_query(select_query,select=True)
        date_of_inv = str(data[0]['snapshot_date'])
        final_data = []
        for record in data:
            final_data.append(Map.shopify_daily_inventory_update(record))
        print("Loading data to Dataframe...",flush=True) 
        df = pd.DataFrame(final_data)
        print(df.head(),flush=True)
        print(f"Inventory Update date is {date_of_inv}")

        print("Converting Dataframe into binary stream...",flush=True)
        csv_file_stream = df_to_csv_bytes(df)

        print("Logging in to Microsoft Services...",flush=True)
        microsoft_login_obj = MicrosoftAzureAppDelegatedLogin(
                                        tenant_id=azure_app_credentials.tenant_id,
                                        grant_type=azure_app_credentials.grant_type,
                                        client_id=azure_app_credentials.client_id,
                                        client_secret_value=azure_app_credentials.client_secret_value,
                                        scope=microsoft_graph_config.scope
                                    )
        print("Retrieving Access Token...",flush=True)
        access_token = microsoft_login_obj.generate_token()

        print("Initiating Sharepoint Drive Object...",flush=True)
        drive_obj = DriveAPI(
                            host=microsoft_graph_config.host,
                            drive_id=sharepoint_config.drive_id,
                            root_to_target_folder_path=sharepoint_config.path,
                            access_token=access_token
                        )
        
        filename = f"Shopify_BBY_Inventory_{date_of_inv}.csv"
        print(f"Uploading file {filename} to sharepoint folder {sharepoint_config.path}...",flush=True)
        response = drive_obj.upload_csv_data(
                                binary_stream=csv_file_stream,
                                sharepoint_folder_path=sharepoint_config.path,
                                filename=filename
                            )
        print(response)
