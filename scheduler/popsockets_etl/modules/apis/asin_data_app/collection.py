import os
import requests
from typing import Literal
from dataclasses import dataclass

@dataclass
class ASINDataApiConfig:
    host: str
    api_key: str

class ASINDataApi:
    def __init__(self,host:str,api_key:str):
        self.host=host
        self.api_key=api_key

    def get_collections(self):
        url = f"{self.host}/collections"
        params = {
            "api_key":self.api_key
        }
        response = requests.get(url=url,params=params)

        if response.status_code==200:
            return response.json()
        else:
            return {"errorCode":response.status_code,"errorDesc":response.json()}
        
    def get_collection_results_set(self,collection_id,result_type:Literal["recent","all"]="all",recent_count=1):
        url = f"{self.host}/collections/{collection_id}/results"
        params = {
            "api_key":self.api_key
        }
        response = requests.get(url=url,params=params)
        if response.status_code==200:
            data = response.json()
            if result_type=="recent":
                return data["results"][:recent_count]
            else:
                return data
        else:
            return {"errorCode":response.status_code,"errorDesc":response.json()}
        
    def get_result_json_zip_file(self,collection_id:str,result_id:int,output_folder_path:str):
        url = f"{self.host}/collections/{collection_id}/results/{result_id}"
        params = {
            "api_key":self.api_key
        }
        response = requests.get(url=url,params=params)
        if response.status_code==200:
            data = response.json()
            zip_file_download_link = data['result']['download_links']['all_pages']
            zip_file_content = requests.get(zip_file_download_link).content
            zip_file_name = f"{collection_id}_{result_id}.zip"
            with open(os.path.join(output_folder_path,zip_file_name),"wb") as f:
                f.write(zip_file_content)
        else:
            return {"errorCode":response.status_code,"errorDesc":response.json()}