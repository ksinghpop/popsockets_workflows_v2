from popsockets_etl.modules.apis.google.gmail import GmailApi
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials, SFCredentialsKeyFile
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, DeleteQuery
from popsockets_etl.modules.transformation.generic import df_remove_totals_row, microsoft_xml_record_list
from popsockets_etl.modules.transformation.barnes_and_nobles.mapping import barnes_and_noble_on_hand_sales_mapping
from popsockets_etl.modules.transformation.force_technologies.mapping import force_technologies_wkly_sales_mapping
from popsockets_etl.modules.transformation.douglas_stewart.mapping import *
from popsockets_etl.modules.transformation.voice_comm.mapping import *
from popsockets_etl.modules.transformation.cesium.mapping import *
from popsockets_etl.modules.transformation.staples.mapping import *
from popsockets_etl.modules.transformation.bestbuy.mapping import *
from popsockets_etl.modules.transformation.kohls.mapping import *
from popsockets_etl.modules.transformation.books_a_million.mapping import *
from popsockets_etl.modules.transformation.valor_comm.mapping import *
from sqlalchemy import String
import datetime as dt
import os
import pandas as pd
import numpy as np
import sys
from dateutil.parser import parse
from typing import Union

def manual_pipeline_flow(gmail_credential_file_path:str,
                    gmail_account_id:str,
                    gmail_from_email:str,
                    gmail_subject:str,
                    snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                    snowflake_tablename:str,
                    start_date:str,
                    end_date:str,
                    data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    start_date_obj = parse(start_date)
    end_date_obj = parse(end_date)
    start_date_str = dt.datetime.strftime(start_date_obj,"%Y/%m/%d")
    end_date_str = dt.datetime.strftime(end_date_obj,"%Y/%m/%d")
    query = f"from:({gmail_from_email}) subject:\"{gmail_subject}\" AND after:{start_date_str} before:{end_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    # for attachment in attachments_metadata:
    #     del attachment['data']
    #     print("-------------------------------------------------",flush=True)
    #     print(attachment,flush=True)
    #     print("-------------------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(books_a_million_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                    sql_engine.execute_query(insert_query,insert=True)

                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True
    


def barnes_and_noble_on_hand_sales_flow(gmail_credential_file_path:str,
                                        gmail_account_id:str,
                                        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                        snowflake_tablename:str,
                                        days_to_check:int,
                                        data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(pmidgette@bn.com) subject:Weekly B&N Sale / On-Hand Summary of POPSOCKETS, POPSOCKETS LLC AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=True)
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(barnes_and_noble_on_hand_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")


def voicecomm_weekly_distribution_flow(gmail_credential_file_path:str,
                                        gmail_account_id:str,
                                        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                        snowflake_tablename:str,
                                        days_to_check:int,
                                        data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(product@myvoicecomm.com) subject:\"Weekly Distribution Report\" -subject:\"VC Weekly Distribution Report\"  AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            # print(orignal_filename)
            # print(orignal_filename_ext)
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=True)
                print(final_df)
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                del attachment['pos']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(voicecomm_wkly_distribution_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")


def voicecomm_weekly_sales_flow(gmail_credential_file_path:str,
                                        gmail_account_id:str,
                                        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                        snowflake_tablename:str,
                                        days_to_check:int,
                                        data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(product@myvoicecomm.com) subject:\"Weekly Sales Report\" -subject:\"VC Weekly Sales Report\"  AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            # print(orignal_filename)
            # print(orignal_filename_ext)
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=True)
                print(final_df)
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(voicecomm_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")


# from:websupport@cesiumtelecom.com subject:"Popsockets sell-through report"
# from:websupport@cesiumtelecom.com subject:"Inventory report for Popsockets" 

def cesium_wkly_sales_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(websupport@cesiumtelecom.com) subject:\"Popsockets sell-through report\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            # print(orignal_filename)
            # print(orignal_filename_ext)
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=True)
                print(final_df)
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(cesium_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")


def cesium_wkly_inventory_flow(gmail_credential_file_path:str,
                                        gmail_account_id:str,
                                        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                        snowflake_tablename:str,
                                        days_to_check:int,
                                        data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(websupport@cesiumtelecom.com) subject:\"Inventory report for Popsockets\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            # print(orignal_filename)
            # print(orignal_filename_ext)
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                del attachment['pos']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(cesium_wkly_inventory_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")


def force_technologies_wkly_sales_flow(gmail_credential_file_path:str,
                                        gmail_account_id:str,
                                        snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                        snowflake_tablename:str,
                                        days_to_check:int,
                                        data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(Fletcher.Coffey@forcetechnology.com.au) subject:\"PopSockets Sell Out Report\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            # print(orignal_filename)
            # print(orignal_filename_ext)
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(force_technologies_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                sql_engine = Engine(snowflake_credentials)
                sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                sql_engine.execute_query(delete_query,delete=True)
                insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                sql_engine.execute_query(insert_query,insert=True)
                print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                print(f"Deleting file from cache...",flush=True)
                os.remove(file_path)
                print(f"Cache cleaned.",flush=True)

        print("====================================",flush=True)
        return True
    
    else:
        print("No Data to Import")

def douglas_stewart_wkly_pos_flow(gmail_credential_file_path:str,
                                    gmail_account_id:str,
                                    snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                    snowflake_tablename:str,
                                    days_to_check:int,
                                    data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(ron.james@dstewart.com) subject:\"Douglas.Stewart.POS.Rpt\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    # for attachment in attachments_metadata:
    #     del attachment['data']
    #     print("-------------------------------------------------",flush=True)
    #     print(attachment,flush=True)
    #     print("-------------------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                # final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False,txt_delimeter='\t')
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(douglas_stewart_wkly_pos_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True


def douglas_stewart_wkly_sales_flow(gmail_credential_file_path:str,
                                    gmail_account_id:str,
                                    snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                                    snowflake_tablename:str,
                                    days_to_check:int,
                                    data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(ron.james@dstewart.com) subject:\"Douglas.Stewart.SELL.Rpt\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    print("Initiating SQL Engine...",flush=True)
    print("------------------------------------",flush=True)
    # for attachment in attachments_metadata:
    #     del attachment['data']
    #     print("-------------------------------------------------",flush=True)
    #     print(attachment,flush=True)
    #     print("-------------------------------------------------",flush=True)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(douglas_stewart_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)

                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True


def staples_wkly_edi852_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(edinotifications@popsockets.com) subject:\"Staples - Weekly Product Activity Report\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    record.update(
                                    {
                                        "Begin Date":parse(record['Begin Date']),
                                        "End Date":parse(record['End Date'])
                                    }
                                )
                    final_import_list.append(staples_wkly_edi852_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list,cast_data_type=[{"name":"vendor_item","type":String}])
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True

def best_buy_wkly_edi852_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(edinotifications@popsockets.com) subject:\"Best Buy - Weekly Product Activity Report\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    record.update(
                                    {
                                        "Begin Date":parse(record['Begin Date']),
                                        "End Date":parse(record['End Date'])
                                    }
                                )
                    final_import_list.append(best_buy_wkly_edi852_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list,cast_data_type=[{"name":"vendor_item","type":String}])
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True


def kohls_wkly_edi852_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(edinotifications@popsockets.com) subject:\"Kohls - Weekly Product Activity Report\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    record.update(
                                    {
                                        "Begin Date":parse(record['Begin Date']),
                                        "End Date":parse(record['End Date'])
                                    }
                                )
                    final_import_list.append(kohls_wkly_edi852_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list,cast_data_type=[{"name":"vendor_item","type":String}])
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True

def books_a_million_wkly_sales_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(infausr@booksamillion.com) subject:\"Books-A-Million Sales Data Extract\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']} | Email Date: {attachment['date']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                final_df['IDate'] = pd.to_datetime(final_df['IDate']).dt.strftime('%Y-%m-%d')
                final_df.replace(np.nan, None,inplace=True)
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(books_a_million_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True


def valor_communications_wkly_sales_flow(gmail_credential_file_path:str,
                            gmail_account_id:str,
                            snowflake_credentials:Union[SFCredentials,SFCredentialsKeyFile],
                            snowflake_tablename:str,
                            days_to_check:int,
                            data_dir:str):
    print("====================================",flush=True)
    print("Initiated Gmail Api...",flush=True)
    print("------------------------------------",flush=True)
    gmail_api = GmailApi(gmail_credential_file_path,gmail_account_id,scopes=['https://mail.google.com/'])
    current_date = dt.date.today()
    start_date = current_date-dt.timedelta(days=days_to_check)
    current_date_str = dt.datetime.strftime(current_date,"%Y/%m/%d")
    start_date_str = dt.datetime.strftime(start_date,"%Y/%m/%d")
    query = f"from:(sellthrough@popsockets.com) subject:\"monthly sell through reports - PopSockets\" AND after:{start_date_str} before:{current_date_str}"
    print(f"Gmail Query:\n{query}")
    print("------------------------------------",flush=True)
    print("Getting Attachment Data...",flush=True)
    print("------------------------------------",flush=True)
    attachments_metadata = gmail_api.get_attachment_files(query=query,output_dir=data_dir)
    if len(attachments_metadata) > 0:
        for attachment in attachments_metadata:
            print("-------------------------------------------------",flush=True)
            print(f"Executing pipeline for {attachment['filename']} | Email Date: {attachment['date']}",flush=True)
            print("-------------------------------------------------",flush=True)
            orignal_filename = attachment['filename']
            orignal_filename_name = os.path.splitext(orignal_filename)[0]
            orignal_filename_ext = os.path.splitext(orignal_filename)[1]
            if orignal_filename_ext in ['.xls','.xlsx','.csv','.xml','.txt','.json']:
                # xlsx_file_name =  f"{os.path.splitext(orignal_filename)[0]}.xlsx"
                # final_df_path = f"{data_dir}/{xlsx_file_name}"
                # xls_to_xlsx2(f"{data_dir}/{orignal_filename}",final_df_path)
                file_path = f"{data_dir}/{orignal_filename_name}_{attachment['pos']}{orignal_filename_ext}"
                final_df = df_remove_totals_row(file_path,ext=orignal_filename_ext,remove_tail=False)
                print(final_df.head())
                # final_df.dropna(axis=0, how='any',inplace=True)
                df_records = final_df.to_dict('records')
                del attachment['data']
                final_import_list = []
                for record in df_records:
                    # for key,value in record.items():
                    #     if value is np.nan:
                    #         record.update({key:None})
                    record.update(attachment)
                    final_import_list.append(valor_communications_wkly_sales_mapping(record))
                print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
                print("------------------------------------",flush=True)
                # final_df.to_sql(snowflake_tablename,con=conx,if_exists='append',index=False)
                # print(final_import_list[0],flush=True)
                if len(final_import_list) == 0:
                    print("No Data to Import",flush=True)
                else:
                    sql_engine = Engine(snowflake_credentials)
                    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
                    delete_query = DeleteQuery(sql_table).delete_by_date(date_column_name='email_recieved_datetime',date_column_value=str(final_import_list[0]['email_recieved_datetime']))
                    sql_engine.execute_query(delete_query,delete=True)
                    insert_query = InsertQuery(sql_table).insert_all(final_import_list)
                    sql_engine.execute_query(insert_query,insert=True)
                    print(f"Total Imported Records: {len(final_import_list)}",flush=True)
                
                try:
                    print(f"Deleting file from cache...",flush=True)
                    os.remove(file_path)
                    print(f"Cache cleaned.",flush=True)
                except Exception as e:
                    print(e)
                    pass
    else:
        print(f"No Attatchment Found",flush=True)
        print("====================================",flush=True)
    return True