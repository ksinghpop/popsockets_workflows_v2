from popsockets_etl.modules.sqlops.snowflake.validator import get_data_type, validate_table_columns
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema, DDLObject
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials
from sqlalchemy import MetaData, Table
from popsockets_etl.modules.sqlops.snowflake.crud import SelectQuery, InsertQuery, DeleteQuery, TruncateTableQuery

def add_missing_column(serialized_dict:dict,tablename:str,sql_engine:Engine,service_type:str):
    """This function created the missing column in mentioned tablename parameter with sql_engine(sqlalchemy engine)"""
    print(f"================================================================================",flush=True)
    conn = sql_engine.get_engine()
    table_obj = SFLoadTableSchema(conn).load_table(tablename)
    missing_columns = validate_table_columns(serialized_dict,table_obj)
    if missing_columns is not False:
        for column in missing_columns:
            stmt = DDLObject.add_column(tablename,column,get_data_type(serialized_dict[column],service_type))
            sql_engine.execute_query(stmt)
            print("----------------------------------------------------------------------------------",flush=True)
        return None
    else:
        return "No column missed"
    
def replicate_tables(src_credential:SFCredentials,
                    dst_credential:SFCredentials,
                    tables:list[str]):
    src_engine = Engine(src_credential)
    dst_engine = Engine(dst_credential)
    if tables is not None:
        metadata = MetaData()
        metadata.reflect(src_engine.get_engine())
        for table in tables:
            print("-----------------------------",flush=True)
            print(f"Loading {table} table from {src_credential.database}|{src_credential.schema} db|schema...",flush=True)
            temp_table:Table = SFLoadTableSchema(src_engine.get_engine()).load_table(table)
            print(f"Creating {table} in {dst_credential.database}|{dst_credential.schema} db|schema...",flush=True)
            temp_table.create(dst_engine.get_engine(),checkfirst=True)
            print(f"{table} created.",flush=True)
            print("-----------------------------",flush=True)
        return True
    else:
        print(f"Either No table name mentioned in tables list parameter or None is passed")
        return False
    
def replicate_schema_all_tables(src_credential:SFCredentials,
                                dst_credential:SFCredentials):
    src_engine = Engine(src_credential)
    dst_engine = Engine(dst_credential)
    metadata = MetaData()
    metadata.reflect(src_engine.get_engine())
    print(f"Replicating all tables from {src_credential.database}|{src_credential.schema} to {dst_credential.database}|{dst_credential.schema}...",flush=True)
    metadata.create_all(dst_engine.get_engine())
    print(f"All tables created.",flush=True)
    return True

def migrate_tables_all_data(src_credential:SFCredentials,
                dst_credential:SFCredentials,
                tables:list[str],
                truncate=False):
    
    src_engine = Engine(src_credential)
    dst_engine = Engine(dst_credential)
    replicate_tables(src_credential,dst_credential,tables)
    for table in tables:
        table_obj = SFLoadTableSchema(src_engine.get_engine()).load_table(table)
        if truncate is True:
            truncate_query = TruncateTableQuery(table_obj).truncate()
            dst_engine.execute_query(truncate_query,truncate=True)
        print(f"Getting data from DB:{src_credential.database}|SCHEMA:{src_credential.schema}|TABLE:{table}...",flush=True)
        limit = 25000
        offset = 0
        while True:
            print(f"Rows Limit: {limit} | Offset: {offset}",flush=True)
            select_query = SelectQuery(table_obj).select_in_chunk(limit=limit,offset=offset)
            data = src_engine.execute_query(select_query,select=True)
            if len(data) > 0:
                print(f"Importing data to DB:{dst_credential.database}|SCHEMA:{dst_credential.schema}|TABLE:{table}...",flush=True)
                insert_query = InsertQuery(table_obj).insert_all(data)
                dst_engine.execute_query(insert_query,insert=True)
                print(f"Data Imported",flush=True)
                offset += limit
            else:
                print("Data Import done.",flush=True)
                break
    return True