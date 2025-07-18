from popsockets_etl.modules.apis.brandfolder.apiv4 import BrandfolderApi, BrandfolderApiConfig, BrandfolderURIs
from popsockets_etl.modules.transformation.brandfolder.mapping import Map
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, DeleteQuery, SelectQuery
import inspect
import datetime as dt

class Test:
    @staticmethod
    def section_asset(
                brandflder_api_config:BrandfolderApiConfig,
                snowflake_credentials:SFCredentials):
        table_name = inspect.currentframe().f_code.co_name
        select_table_name = 'section'
        sql_engine = Engine(snowflake_credentials)
        sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(table_name)
        print(f"Getting data from {select_table_name} table...",flush=True)
        select_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(select_table_name)
        select_query = SelectQuery(select_table).select_specific_columns(['id'])
        record_ids = sql_engine.execute_query(select_query,select=True)
        params = brandflder_api_config.params
        api_key = brandflder_api_config.api_key
        pos = 1
        for record_id in record_ids:
            print("================================================")
            print("================================================")
            print(f"Getting {table_name.capitalize()} data from Branfolder API for {select_table_name} ({pos}): {record_id['id']}",flush=True)
            url = BrandfolderURIs.get_assets_in_section(record_id['id'])
            loop_status = True
            page = 1
            while loop_status is True:
                final_data = []
                params.page = page
                api_obj = BrandfolderApi(api_key)
                print(f"Calling {table_name.capitalize()} Endpoint with following config:\nurl:{url}\nparams:{params}",flush=True)
                data = api_obj.call(method='get',url=url,params=params)
                records = data['data']
                included_data = ['included']
                included_record_pos = 0
                all_record_ids = []
                for record in records:
                    custom_field_list = []
                    for cf in record['relationships']['custom_field_values']['data']:
                        custom_field_list.append(included_data[included_record_pos])
                        included_record_pos += 1
                    final_data.append(Map.section_asset(record,custom_field_list,record_id['id']))
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