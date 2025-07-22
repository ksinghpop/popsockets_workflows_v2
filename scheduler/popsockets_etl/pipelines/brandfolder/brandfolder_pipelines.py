from popsockets_etl.modules.apis.brandfolder.apiv4 import BrandfolderApi, BrandfolderApiConfig, BrandfolderURIs, process_included_data
from popsockets_etl.modules.transformation.brandfolder.mapping import Map
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials, SFCredentialsKeyFile
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema, create_table_from_dataframe, DDLObject
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, DeleteQuery, SelectQuery
from popsockets_etl.modules.transformation.generic import chunk_list
import inspect
from typing import Union
import datetime as dt
import json
import pandas as pd
import numpy as np

def organisation(
                brandflder_api_config:BrandfolderApiConfig,
                snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    table_name = inspect.currentframe().f_code.co_name
    sql_engine = Engine(snowflake_credentials)
    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(table_name)
    print(f"{table_name.capitalize()} Job Initiated...",flush=True)
    print("================================================")
    url = brandflder_api_config.uri
    params = brandflder_api_config.params
    api_key = brandflder_api_config.api_key
    loop_status = True
    
    page = 1
    while loop_status is True:
        final_data = []
        params.page = page
        api_obj = BrandfolderApi(api_key)
        print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
        data = api_obj.call(method='get',url=url,params=params)
        records = data['data']
        all_record_ids = []
        for record in records:
            final_data.append(Map.organisation(record))
            all_record_ids.append(record['id'])
        print(f"Total Records for API Call:{len(final_data)}",flush=True)
        print("-------------------------------------------",flush=True)
        if len(final_data) > 0:
            print(f"Importing data to snowflake...",flush=True)
            delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
            sql_engine.execute_query(delete_query,delete=True)
            insert_query = InsertQuery(sql_table).insert_all(final_data)
            sql_engine.execute_query(insert_query,insert=True)
            print(f"Import done for page {params.page}",flush=True)
            print("================================================")
        else:
            print("No Record Found",flush=True)
        
        page += 1

        if data['meta']['next_page'] is None:
            loop_status = False

    return True

def brandfolder(
                brandflder_api_config:BrandfolderApiConfig,
                snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    table_name = inspect.currentframe().f_code.co_name
    sql_engine = Engine(snowflake_credentials)
    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(table_name)
    print(f"{table_name.capitalize()} Job Initiated...",flush=True)
    print("================================================")
    url = brandflder_api_config.uri
    params = brandflder_api_config.params
    api_key = brandflder_api_config.api_key
    loop_status = True
    
    page = 1
    while loop_status is True:
        final_data = []
        params.page = page
        api_obj = BrandfolderApi(api_key)
        print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
        data = api_obj.call(method='get',url=url,params=params)
        records = data['data']
        all_record_ids = []
        for record in records:
            final_data.append(Map.brandfolder(record,"of8xj0-3srp34-doq5w8"))
            all_record_ids.append(record['id'])
        print(f"Total Records for API Call:{len(final_data)}",flush=True)
        print("-------------------------------------------",flush=True)
        if len(final_data) > 0:
            print(f"Importing data to snowflake...",flush=True)
            delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
            sql_engine.execute_query(delete_query,delete=True)
            insert_query = InsertQuery(sql_table).insert_all(final_data)
            sql_engine.execute_query(insert_query,insert=True)
            print(f"Import done for page {params.page}",flush=True)
            print("================================================")
        else:
            print("No Record Found",flush=True)
        
        page += 1

        if data['meta']['next_page'] is None:
            loop_status = False

    return True

def section(
            brandflder_api_config:BrandfolderApiConfig,
            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    table_name = inspect.currentframe().f_code.co_name
    select_table_name = 'brandfolder'
    sql_engine = Engine(snowflake_credentials)
    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(table_name)
    print(f"Getting data from {select_table_name} table...",flush=True)
    brandfolder_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(select_table_name)
    select_query = SelectQuery(brandfolder_table).select_specific_columns(['id'])
    brandfolder_ids = sql_engine.execute_query(select_query,select=True)
    params = brandflder_api_config.params
    api_key = brandflder_api_config.api_key
    for brandfolder_id in brandfolder_ids:
        print("================================================")
        print("================================================")
        print(f"Getting {table_name.capitalize()} data from Branfolder API for brandfolder: {brandfolder_id['id']}",flush=True)
        url = BrandfolderURIs.get_sections_in_brandfolders(brandfolder_id['id'])
        loop_status = True
        page = 1
        while loop_status is True:
            final_data = []
            params.page = page
            api_obj = BrandfolderApi(api_key)
            print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
            data = api_obj.call(method='get',url=url,params=params)
            records = data['data']
            all_record_ids = []
            for record in records:
                final_data.append(Map.section(record,brandfolder_id['id']))
                all_record_ids.append(record['id'])
            print(f"Total Records for API Call:{len(final_data)}",flush=True)
            print("-------------------------------------------",flush=True)
            if len(final_data) > 0:
                print(f"Importing data to snowflake...",flush=True)
                delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_data)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Import done for page {params.page}",flush=True)
                print("================================================")
            else:
                print("No Record Found",flush=True)
            
            page += 1

            if data['meta']['next_page'] is None:
                loop_status = False
    return True

def asset(
        brandflder_api_config:BrandfolderApiConfig,
        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
        table_name = inspect.currentframe().f_code.co_name
        select_table_name = 'brandfolder'
        sql_engine = Engine(snowflake_credentials)
        sql_engine_loaded = sql_engine.get_engine()
        sql_table = SFLoadTableSchema(sql_engine_loaded).load_table(table_name)
        print(f"Getting data from {select_table_name} table...",flush=True)
        select_table = SFLoadTableSchema(sql_engine_loaded).load_table(select_table_name)
        select_query = SelectQuery(select_table).select_specific_columns(['id'])
        record_ids = sql_engine.execute_query(select_query,select=True)
        params = brandflder_api_config.params
        api_key = brandflder_api_config.api_key
        pos = 1
        for record_id in record_ids:
            print("================================================")
            print("================================================")
            print(f"Getting {table_name.capitalize()} data from Branfolder API for {select_table_name} ({pos}): {record_id['id']}",flush=True)
            url = BrandfolderURIs.get_assets_in_brandfolder(record_id['id'])
            loop_status = True
            page = 1
            while loop_status is True:
                final_data = []
                params.page = page
                api_obj = BrandfolderApi(api_key)
                print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
                data = api_obj.call(method='get',url=url,params=params)
                records = data['data']
                included_key_status = False
                try:
                    included_data = data['included']
                    included_key_status = True
                except:
                    pass
                if included_key_status is True:
                    included_data_processed = process_included_data(included_data)
                all_record_ids = []
                for record in records:
                    section_id = record['relationships']['section']['data']['id']
                    collection_ids = record['relationships']['collections']['data']
                    cf_data = record['relationships']['custom_field_values']['data']
                    tags_data = record['relationships']['tags']['data']
                    custom_field_list = [included_data_processed['custom_field_values'][cf['id']] for cf in cf_data]
                    tags_list = [included_data_processed['tags'][tag['id']] for tag in tags_data]
                    final_data.append(Map.asset(record,section_id,collection_ids,custom_field_list,tags_list))
                    all_record_ids.append(record['id'])
                print(f"Total Records for API Call:{len(final_data)}",flush=True)
                print("-------------------------------------------",flush=True)
                if len(final_data) > 0:
                    print(f"Importing data to snowflake...",flush=True)
                    delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_data)
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Import done for page {params.page}",flush=True)
                    print("================================================")
                else:
                    print("No Record Found",flush=True)
                
                page += 1

                if data['meta']['next_page'] is None:
                    loop_status = False
            pos += 1
        return True

def attachment(
            brandflder_api_config:BrandfolderApiConfig,
            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    table_name = inspect.currentframe().f_code.co_name
    select_table_name = 'brandfolder'
    sql_engine = Engine(snowflake_credentials)
    sql_engine_loaded = sql_engine.get_engine()
    sql_table = SFLoadTableSchema(sql_engine_loaded).load_table(table_name)
    print(f"Getting data from {select_table_name} table...",flush=True)
    select_table = SFLoadTableSchema(sql_engine_loaded).load_table(select_table_name)
    select_query = SelectQuery(select_table).select_specific_columns(['id'])
    record_ids = sql_engine.execute_query(select_query,select=True)
    params = brandflder_api_config.params
    api_key = brandflder_api_config.api_key
    pos = 1
    for record_id in record_ids:
        print("================================================")
        print("================================================")
        print(f"Getting {table_name.capitalize()} data from Branfolder API for {select_table_name} ({pos}): {record_id['id']}",flush=True)
        url = BrandfolderURIs.get_attachments_in_brandfolder(record_id['id'])
        loop_status = True
        page = 1
        while loop_status is True:
            final_data = []
            params.page = page
            api_obj = BrandfolderApi(api_key)
            print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
            data = api_obj.call(method='get',url=url,params=params)
            records = data['data']
            all_record_ids = []
            for record in records:
                asset_id = record['relationships']['asset']['data']['id']
                final_data.append(Map.attachment(record,asset_id))
                all_record_ids.append(record['id'])
            print(f"Total Records for API Call:{len(final_data)}",flush=True)
            print("-------------------------------------------",flush=True)
            if len(final_data) > 0:
                print(f"Importing data to snowflake...",flush=True)
                delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_data)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Import done for page {params.page}",flush=True)
                print("================================================")
            else:
                print("No Record Found",flush=True)
            
            page += 1

            if data['meta']['next_page'] is None:
                loop_status = False
        pos += 1
    return True

def asset_custom_fields(snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],select_table_last_hours:int=None):
    table_name = inspect.currentframe().f_code.co_name
    select_table_name = 'asset'
    sql_engine = Engine(snowflake_credentials)
    sql_engine_loaded = sql_engine.get_engine()
    sql_table = SFLoadTableSchema(sql_engine_loaded).load_table(table_name)
    print(f"Getting data from {select_table_name} table...",flush=True)
    select_table = SFLoadTableSchema(sql_engine_loaded).load_table(select_table_name)
    columns_to_be_selected = ['id','custom_field_values']
    select_query = None
    if select_table_last_hours is not None:
        current_datetime = dt.datetime.now()
        end_time_str = dt.datetime.strftime(current_datetime,"%Y-%m-%d %H:%M:%S")
        start_datetime = current_datetime - dt.timedelta(hours=select_table_last_hours)
        start_time_str = dt.datetime.strftime(start_datetime,"%Y-%m-%d %H:%M:%S")
        select_query = SelectQuery(select_table).select_specific_columns_by_daterange(columns=columns_to_be_selected,start_date=start_time_str,end_date=end_time_str,start_date_column='ATTRIBUTES_UPDATED_AT',end_date_column='ATTRIBUTES_UPDATED_AT',desc_sort_column=select_table.c.attributes_updated_at)
    else:
        select_query = SelectQuery(select_table).select_specific_columns(columns=columns_to_be_selected,desc_sort_column=select_table.c.attributes_updated_at)
    parent_data = sql_engine.execute_query(select_query,select=True)
    
    final_data = []
    parent_record_ids = []
    print("================================================",flush=True)
    print(f"Serializing Data...",flush=True)
    for parent_record in parent_data:
        line_data = json.loads(parent_record['custom_field_values'])
        if len(line_data) > 0:
            for line_record in line_data:
                final_data.append(Map.asset_custom_fields(line_record,parent_record['id']))
                parent_record_ids.append(parent_record['id'])

    insert_record_count = len(final_data)
    if insert_record_count > 0:
        print(f"Importing data to snowflake...",flush=True)
        delete_query = DeleteQuery(sql_table).delete_by_ids_list('asset_id',parent_record_ids)
        sql_engine.execute_query(delete_query,delete=True)
        insert_query = InsertQuery(sql_table).insert_all(final_data)
        sql_engine.execute_query(insert_query,insert=True)
        print(f"Import done for {insert_record_count}")
        print("================================================")
    else:
        print("No Record Found",flush=True)

    return True

def asset_collections(snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],select_table_last_hours:int=None):
    table_name = inspect.currentframe().f_code.co_name
    select_table_name = 'asset'
    sql_engine = Engine(snowflake_credentials)
    sql_engine_loaded = sql_engine.get_engine()
    sql_table = SFLoadTableSchema(sql_engine_loaded).load_table(table_name)
    print(f"Getting data from {select_table_name} table...",flush=True)
    select_table = SFLoadTableSchema(sql_engine_loaded).load_table(select_table_name)
    columns_to_be_selected = ['id','collection_ids']
    select_query = None
    if select_table_last_hours is not None:
        current_datetime = dt.datetime.now()
        end_time_str = dt.datetime.strftime(current_datetime,"%Y-%m-%d %H:%M:%S")
        start_datetime = current_datetime - dt.timedelta(hours=select_table_last_hours)
        start_time_str = dt.datetime.strftime(start_datetime,"%Y-%m-%d %H:%M:%S")
        select_query = SelectQuery(select_table).select_specific_columns_by_daterange(columns=columns_to_be_selected,start_date=start_time_str,end_date=end_time_str,start_date_column='ATTRIBUTES_UPDATED_AT',end_date_column='ATTRIBUTES_UPDATED_AT',desc_sort_column=select_table.c.attributes_updated_at)
    else:
        select_query = SelectQuery(select_table).select_specific_columns(columns=columns_to_be_selected,desc_sort_column=select_table.c.attributes_updated_at)
    parent_data = sql_engine.execute_query(select_query,select=True)
    
    final_data = []
    parent_record_ids = []
    print("================================================",flush=True)
    print(f"Serializing Data...",flush=True)
    for parent_record in parent_data:
        line_data = json.loads(parent_record['collection_ids'])
        if len(line_data) > 0:
            for line_record in line_data:
                final_data.append(Map.asset_collections(parent_record['id'],line_record['id']))
                parent_record_ids.append(parent_record['id'])

    insert_record_count = len(final_data)
    if insert_record_count > 0:
        print(f"Importing data to snowflake...",flush=True)
        delete_query = DeleteQuery(sql_table).delete_by_ids_list('asset_id',parent_record_ids)
        sql_engine.execute_query(delete_query,delete=True)
        insert_query = InsertQuery(sql_table).insert_all(final_data)
        sql_engine.execute_query(insert_query,insert=True)
        print(f"Import done for {insert_record_count}")
        print("================================================")
    else:
        print("No Record Found",flush=True)

    return True

def collection(
            brandflder_api_config:BrandfolderApiConfig,
            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    table_name = inspect.currentframe().f_code.co_name
    select_table_name = 'brandfolder'
    sql_engine = Engine(snowflake_credentials)
    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(table_name)
    print(f"Getting data from {select_table_name} table...",flush=True)
    brandfolder_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(select_table_name)
    select_query = SelectQuery(brandfolder_table).select_specific_columns(['id'])
    brandfolder_ids = sql_engine.execute_query(select_query,select=True)
    params = brandflder_api_config.params
    api_key = brandflder_api_config.api_key
    for brandfolder_id in brandfolder_ids:
        print("================================================")
        print("================================================")
        print(f"Getting {table_name.capitalize()} data from Branfolder API for brandfolder: {brandfolder_id['id']}",flush=True)
        url = BrandfolderURIs.get_collection_in_brandfolders(brandfolder_id['id'])
        loop_status = True
        page = 1
        while loop_status is True:
            final_data = []
            params.page = page
            api_obj = BrandfolderApi(api_key)
            print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
            data = api_obj.call(method='get',url=url,params=params)
            records = data['data']
            all_record_ids = []
            for record in records:
                final_data.append(Map.collection(record,brandfolder_id['id']))
                all_record_ids.append(record['id'])
            print(f"Total Records for API Call:{len(final_data)}",flush=True)
            print("-------------------------------------------",flush=True)
            if len(final_data) > 0:
                print(f"Importing data to snowflake...",flush=True)
                delete_query = DeleteQuery(sql_table).delete_by_ids_list('id',all_record_ids)
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_data)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Import done for page {params.page}",flush=True)
                print("================================================")
            else:
                print("No Record Found",flush=True)
            
            page += 1

            if data['meta']['next_page'] is None:
                loop_status = False
    return True

def brandfolder_sku_urls(snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile]):
    print("Extracting data...")
    engine = Engine(snowflake_credentials)
    engine_loaded = engine.get_engine()
    query = """select asset_id,ATTRIBUTES_POSITION,ATTRIBUTES_CDN_URL from prod.brandfolder.attachment"""
    # columns_list = ["asset_id","attributes_position","attributes_cdn_url"]
    # sql_table = SFLoadTableSchema(engine_loaded).load_table("attachment")
    query = SelectQuery.select_by_query_str(query)
    data = engine.execute_query(query,select=True)
    print("=======================================")
    print("Transforming data...")
    df = pd.DataFrame(data)
    df['unique_code'] = df['asset_id']+"_"+df['attributes_cdn_url']
    df.sort_values(by='attributes_position', ascending=True, inplace=True)
    df['cdn_url_column'] = df.groupby('asset_id').cumcount() + 1
    pivot_df = df.pivot(index='asset_id', columns=f'cdn_url_column', values='attributes_cdn_url')
    pivot_df.columns = [f'cdn_url_{int(col)}' for col in pivot_df.columns]
    pivot_df.reset_index(inplace=True)
    pivot_df.replace(np.nan,None,inplace=True)
    data_dict = pivot_df.to_dict(orient='records',index=True)
    print("========================================")
    print("Validating Schema...")
    sql_table = create_table_from_dataframe(pivot_df,"brandfolder_sku_urls")
    DDLObject.create_table(sql_table,conn=engine_loaded,replace=True)
    print("========================================")
    print("Creating chunks of the data...")
    chunk_size = 3000
    data_chunk_list = list(chunk_list(data_dict,chunk_size))
    chunks_count = len(data_chunk_list)
    print(f"Chunks created: {chunks_count}")
    print("========================================")
    print("Importing Data to database...")
    chunk_pos = 1
    for chunk in data_chunk_list:
        insert_query = InsertQuery(sql_table).insert_all(chunk)
        engine.execute_query(insert_query,insert=True)
        print("---------------------------------------")
        print(f"Data Imported for chunk:{chunk_pos}")
        chunk_pos += 1
    print("---------------------------------------")
    # insert_query = InsertQuery(sql_table).insert_all(data_dict)
    # engine.execute_query(insert_query,insert=True)
    # print(f"Data Imported with records:{len(data_dict)}")
    print("========================================")
    print(f"Data Imported for all chunks with following details:\nChunks:{chunks_count}\nChunkSize:{chunk_size}\nTotal Records:{len(data_dict)}")
    return True