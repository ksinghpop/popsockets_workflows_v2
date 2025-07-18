from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, DeleteQuery
from popsockets_etl.modules.transformation.generic import df_remove_totals_row, microsoft_xml_record_list
from popsockets_etl.modules.transformation.bestbuy.mapping import *
from sqlalchemy import String
import datetime as dt
import os
import pandas as pd
import numpy as np
import sys
from dateutil.parser import parse
import os

def best_buy_wkly_edi852_manual(
                            file_path:str,
                            snowflake_credentials:SFCredentials,
                            snowflake_tablename:str
                            ):
    
    file_str = os.path.basename(file_path)
    file_name, extension = os.path.splitext(file_str)
    final_df = df_remove_totals_row(file_path,ext=extension,remove_tail=False)
    print(final_df.head())
    df_records = final_df.to_dict('records')
    final_import_list = []
    for record in df_records:
        record.update(
                        {
                            "Begin Date":parse(record['Begin Date']),
                            "End Date":parse(record['End Date']),
                            "filename": file_str
                        }
                    )
        final_import_list.append(best_buy_wkly_edi852_mapping(record,manual_job=True))
    print(len(final_import_list),flush=True)
    print(f"Importing Data to SQL DB in {snowflake_tablename} table...",flush=True)
    print("------------------------------------",flush=True)
    if len(final_import_list) == 0:
        print("No Data to Import",flush=True)
    else:
        sql_engine = Engine(snowflake_credentials)
        sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(snowflake_tablename)
        insert_query = InsertQuery(sql_table).insert_all(final_import_list,cast_data_type=[{"name":"vendor_item","type":String}])
        sql_engine.execute_query(insert_query,insert=True)
        print(f"Total Imported Records: {len(final_import_list)}",flush=True)
    
    return True