import os
import json
import pandas as pd
import numpy as np
import datetime as dt
from popsockets_etl.modules.apis.asin_data_app.collection import ASINDataApi, ASINDataApiConfig
from popsockets_etl.modules.transformation.asin_data_app.mapping import Map
from popsockets_etl.modules.sqlops.snowflake.credentials import SFCredentials
from popsockets_etl.modules.sqlops.snowflake.engine import Engine
from popsockets_etl.modules.sqlops.snowflake.schema import SFLoadTableSchema
from popsockets_etl.modules.sqlops.snowflake.crud import InsertQuery, TruncateTableQuery, SelectQuery
from popsockets_etl.modules.utils import ensure_directory_exists, empty_folder, load_and_extract_zip
from popsockets_etl.modules.popsockets_ecom.contentup import evaluate
from popsockets_etl.modules.generate_uid import ObjectIdGenerator
from sqlalchemy import text

def update_collection_data(collection_id:str,
                      api_config:ASINDataApiConfig,
                      snowflake_credentials:SFCredentials,
                      tablename:str,
                      zip_file_location:str
                      ):
    print("Initiating ASIN Data Collection API Object...",flush=True)
    # Initiate ASIN Collection API Object
    api_obj = ASINDataApi(api_config.host,api_config.api_key)

    print(f"Getting recent results id for collection {collection_id}...")
    # Get the latest result set id fo the collection
    recent_result_record = api_obj.get_collection_results_set(
                                        collection_id=collection_id,
                                        result_type='recent'
                                                )
    latest_result_set_id = recent_result_record[0]["id"]
    print(f"Got the result id: {latest_result_set_id}")

    print("Ensuring the zip file folder path...")
    ensure_directory_exists(zip_file_location)

    print(f"Downloading zip file from Api on location {zip_file_location}...")
    api_obj.get_result_json_zip_file(
                                collection_id=collection_id,
                                result_id=latest_result_set_id,
                                output_folder_path=zip_file_location
                            )
    
    # Extracting the zip file
    print(f"Extracting the zip file from {zip_file_location}...")
    load_and_extract_zip(
                        zip_src_path=os.path.join(zip_file_location,f"{collection_id}_{latest_result_set_id}.zip"),
                        extract_path=zip_file_location,
                        delete_zip=True
                        )

    # SQL Config
    sql_engine = Engine(snowflake_credentials)
    sql_table = SFLoadTableSchema(sql_engine.get_engine()).load_table(tablename)
    truncate_query = TruncateTableQuery(sql_table).truncate()
    sql_engine.execute_query(truncate_query,truncate=True)

    total_records = 0
    for file in os.listdir(zip_file_location):
        print("==================================================================",flush=True)
        if file.endswith(".json"):
            json_file_path = os.path.join(zip_file_location, file)
            with open(json_file_path, 'r') as json_file:
                data = json.load(json_file)
            final_records = [Map.product(record) for record in data]
            insert_query = InsertQuery(sql_table).insert_all(final_records)
            sql_engine.execute_query(insert_query,insert=True)
            total_records += len(data)
            print(f"Inserted {len(data)} records into {tablename} from {json_file_path}")
            print("==================================================================",flush=True)

    print(f"Total records inserted into {tablename} from {zip_file_location}: {total_records}")

    # Clean up the folder after processing
    print(f"Cleaning up the folder {zip_file_location}...")
    empty_folder(zip_file_location)
    os.rmdir(zip_file_location)

    print("***************************End*********************************",flush=True)

    return True


def evaluate_amazon_data(
        snowflake_credentials:SFCredentials,
        amazon_domain: str,
        live_data_view:str,
        validation_data_view:str
):
    evaluation_id = ObjectIdGenerator().generate()
    print(f"EvaluationId:{evaluation_id} started...",flush=True)
    evaluation_start_utc = dt.datetime.now(dt.timezone.utc)
    # evaluation_start_utc = current_time.strftime("'%Y-%m-%d %H:%M:%S'")
    sql_engine = Engine(snowflake_credentials)
    
    with sql_engine.get_engine().begin() as conn:
        print("Loading Live ASIN data into dataframe...",flush=True)
        live_collection_data_query = text(f"""select * from {live_data_view} where amazon_domain = '{amazon_domain}'""")
        live_collection_data_dict = sql_engine.execute_query(live_collection_data_query,select=True)

        print("Loading Validation data into dataframe...",flush=True)
        validation_data_query = None
        if amazon_domain in ('amazon.ca','amazon.co.uk','amazon.de','amazon.com','amazon.in'):
            validation_data_query = text(f"""select * from {validation_data_view} where amazon_domain = 'amazon.com'""")
        else:
            validation_data_query = text(f"""select * from {validation_data_view} where amazon_domain = '{amazon_domain}'""")
        vaidation_data_dict = sql_engine.execute_query(validation_data_query,select=True)
        vaidation_data_df = pd.DataFrame(vaidation_data_dict)
        vaidation_data_new_df = vaidation_data_df.replace(np.nan,None)
        print(f"Validation data Datafeame:\n{vaidation_data_new_df.head()}",flush=True)

    validation_data_asins = vaidation_data_new_df['asin'].dropna().unique().tolist()

    contentup_evaluation_asins_data = []
    contentup_evaluation_asin_results_data = []

    asins_count = 0
    evaluated_asins = 0
    not_found_asins = 0
    for record in live_collection_data_dict:
        asins_count += 1
        print(f"Evaluation going on: {asins_count}\nASIN: {record['asin']}",end="\r")
        if record['asin'] in validation_data_asins:
            evaluated_asins += 1
            validation_record = vaidation_data_new_df[vaidation_data_new_df['asin']==record['asin']].iloc[0].to_dict()
            # print(f"Live Record:\n{record}",end="\r",flush=True)
            # print(f"Validation Record:\n{validation_record}",end="\r",flush=True)
            eval_result = evaluate(
                validation_data_df=vaidation_data_new_df,
                asin_collection_api_data=record,
                master_validation_data=validation_record
            )
            evaluation_asin_id = ObjectIdGenerator().generate()
            evaluation_asin = {
                    "id": evaluation_asin_id,
                    "asin": eval_result['asin'],
                    "score": eval_result['score'],
                    "contentup_evaluation_id": evaluation_id,
                    "timestamp": dt.datetime.now(dt.timezone.utc)
                }
            evaluation_asin_results = []
            for record in eval_result['results']:
                _dict = record.update({
                    "id": ObjectIdGenerator().generate(),
                    "contentup_evaluation_asin_id":evaluation_asin_id,
                    "timestamp": dt.datetime.now(dt.timezone.utc)
                })
                contentup_evaluation_asin_results_data.append(record)
            
            contentup_evaluation_asins_data.append(evaluation_asin)
        else:
            not_found_asins += 0
            contentup_evaluation_asins_data.append({
                    "id": ObjectIdGenerator().generate(),
                    "asin": record['asin'],
                    "score": None,
                    "contentup_evaluation_id": evaluation_id,
                    "timestamp": dt.datetime.now(dt.timezone.utc)
                })
            
    print(f"Total ASINS: {asins_count}\Evaluated ASINS: {evaluated_asins}\nNot Found in Validation Data: {not_found_asins}",flush=True)
    
    evaluation_end_utc = dt.datetime.now(dt.timezone.utc)

    # print(contentup_evaluation_asin_results_data[0])

    print("Inserting data to snowflake...",flush=True)
    contentup_evaluation_table = SFLoadTableSchema(sql_engine.get_engine()).load_table("contentup_evaluation")
    # contentup_evaluation_truncate_query = TruncateTableQuery(contentup_evaluation_table).truncate()
    contentup_evaluation_insert_query = InsertQuery(contentup_evaluation_table).insert({
        "id": evaluation_id,
        "amazon_domain": amazon_domain,
        "start_utc": evaluation_start_utc,
        "end_utc": evaluation_end_utc
    })
    # sql_engine.execute_query(contentup_evaluation_truncate_query,truncate=True)
    sql_engine.execute_query(contentup_evaluation_insert_query,insert=True)
    print("Evaluation Id insertion done.",flush=True)


    if len(contentup_evaluation_asins_data)>0:
        contentup_evaluation_asins_table = SFLoadTableSchema(sql_engine.get_engine()).load_table("contentup_evaluation_asins")
        # contentup_evaluation_asins_truncate_query = TruncateTableQuery(contentup_evaluation_asins_table).truncate()
        contentup_evaluation_asins_query = InsertQuery(contentup_evaluation_asins_table).insert_all(contentup_evaluation_asins_data)
        # sql_engine.execute_query(contentup_evaluation_asins_truncate_query,truncate=True)
        sql_engine.execute_query(contentup_evaluation_asins_query,insert=True)
    else:
        print("No ASIN Evaluation to import")


    if len(contentup_evaluation_asin_results_data)>0:
        contentup_evaluation_asin_results_table = SFLoadTableSchema(sql_engine.get_engine()).load_table("contentup_evaluation_asin_results")
        # contentup_evaluation_asin_results_truncate_query = TruncateTableQuery(contentup_evaluation_asin_results_table).truncate()
        contentup_evaluation_asin_results_query = InsertQuery(contentup_evaluation_asin_results_table).insert_all(contentup_evaluation_asin_results_data)
        # sql_engine.execute_query(contentup_evaluation_asin_results_truncate_query,truncate=True)
        sql_engine.execute_query(contentup_evaluation_asin_results_query,insert=True)
    else:
        print("No ASIN Evaluation result to import")

    return True

def delete_old_contentup_data(
        snowflake_credentials:SFCredentials,
        asin_evaluation_days:int,
        asin_results_evaluation_days:int,
        ):
    sql_engine = Engine(snowflake_credentials)
    sql_engine.execute_query(text(f"""delete from contentup_evaluation_asins where timestamp < DATEADD(DAY, -{asin_evaluation_days}, CURRENT_TIMESTAMP())"""),delete=True)
    sql_engine.execute_query(text(f"""delete from contentup_evaluation_asin_results where timestamp < DATEADD(DAY, -{asin_results_evaluation_days}, CURRENT_TIMESTAMP())"""),delete=True)
    
    return True