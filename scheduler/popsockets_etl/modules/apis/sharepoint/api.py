import requests
import os
from dataclasses import dataclass
from dateutil.parser import parse
import datetime as dt
import json
from popsockets_etl.modules.transformation.sharepoint.transform import df_to_csv_bytes


class SharepointApiFailException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code
        super().__init__(f"Sharepoint API Failed\nErrorCode:{code}\nError Description:{message}")

@dataclass
class AzureAppCredentials:
    tenant_id:str
    client_id:str
    client_secret_value:str
    grant_type:str
    client_secret_id:str=None

@dataclass
class MicrosoftGraphApiConfig:
    host:str
    scope:str
    filter:str=None
    days:int=None

@dataclass
class SharepointConfig:
    path:str
    site_id:str=None
    drive_id:str=None

class MicrosoftAzureAppDelegatedLogin:
    def __init__(self,tenant_id:str,grant_type:str,client_id:str,client_secret_value:str,scope:str):
        self.tenant_id=tenant_id
        self.client_id=client_id
        self.client_secret_value=client_secret_value
        self.grant_type=grant_type
        self.scope=scope

    def generate_token(self):
        login_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        response = requests.get(
                            url=login_url,
                            data={
                                "client_id":self.client_id,
                                "client_secret":self.client_secret_value,
                                "grant_type":self.grant_type,
                                "scope":self.scope
                            }
                        )
        if response.status_code==200:
            token_data = response.json()
            return token_data['access_token']
        else:
            raise SharepointApiFailException(message=json.dumps(response.text),code=response.status_code)
        
class DriveAPI:
    def __init__(self,host:str,drive_id:str,root_to_target_folder_path:str,access_token:str):
        self.host=host
        self.drive_id=drive_id
        self.root_to_target_folder_path=root_to_target_folder_path
        self.access_token=access_token

    def get_content_by_folder_path(self,sort_field:str="lastModifiedDateTime",sort_type:str="desc",filter:str="",days:int=None):
        url = f"{self.host}/drives/{self.drive_id}/root:/{self.root_to_target_folder_path}:/children"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {
            "$orderby":f"{sort_field} {sort_type}",
            "$filter":filter
        }
        print(url,flush=True)
        all_data = []
        loop_status = True
        while url is not None and loop_status is True:
            response = requests.get(
                                    url=url,
                                    params=params,
                                    headers=headers
                                )
            if response.status_code==200:
                data = response.json()
                if days is not None:
                    current_date = dt.datetime.now(dt.timezone.utc)
                    start_date = current_date-dt.timedelta(days=days)
                    for record in data['value']:
                        record_last_modified_date = parse(record['lastModifiedDateTime'])
                        if record_last_modified_date >= start_date:
                            all_data.append(record)
                        else:
                            loop_status = False
                            break
                    try:
                        url = data['@odata.nextLink']
                    except:
                        url = None
                else:
                    all_data.extend(data['value'])
                    try:
                        url = data['@odata.nextLink']
                    except:
                        url = None
            else:
                raise SharepointApiFailException(message=json.dumps(response.text),code=response.status_code)

        return all_data
    
    def get_data(self,sharepoint_path:str,filename:str):
        url = f"{self.host}/{sharepoint_path}/{filename}:/content"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url=url,headers=headers)
        if response.status_code==200:
            return response.content
        else:
            raise SharepointApiFailException(message=json.dumps(response.text),code=response.status_code)
        
    def download_data(self,sharepoint_path:str,filename:str,output_path:str):
        data = self.get_data(sharepoint_path,filename)
        with open(os.path.join(output_path,filename),'wb') as f:
            f.write(data)

    def upload_csv_data(self,binary_stream:bytes,sharepoint_folder_path:str,filename:str):
        url = f"{self.host}/drives/{self.drive_id}/root:/{sharepoint_folder_path}/{filename}:/content"
        headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "text/csv"
                }
        response = requests.put(url=url,headers=headers,data=binary_stream)
        if response.status_code==201:
            return {
                "status_code": response.status_code,
                "status_desc":"New file uploaded"
                }
        elif response.status_code==200:
            {
                "status_code": response.status_code,
                "status_desc":"Existing File Updated"
                }
        else:
            raise SharepointApiFailException(message=json.dumps(response.text),code=response.status_code)