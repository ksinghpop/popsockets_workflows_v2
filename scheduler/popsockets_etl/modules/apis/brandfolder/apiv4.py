import requests
from dataclasses import dataclass, fields
from urllib.parse import quote_plus
from dateutil.parser import parse
from popsockets_etl.modules.timelineops import ValidateTimelineUTC
# class BrandfolderApi:
#     base_url = "https://brandfolder.com/api/v4"

#     def __init__(endpoint:str,params:dict,page:int,per_page:int):
#         pass

BASE_URL = "https://brandfolder.com/api/v4"

class BrandfolderURIs:
    
    def get_organisations():
        return f"{BASE_URL}/organizations"
    
    def get_brandfolders():
        return f"{BASE_URL}/brandfolders"

    def get_brandfolders_in_organisation(organisation_id:str):
        return f"{BASE_URL}/organizations/{organisation_id}/brandfolders"
    
    def get_assets_in_brandfolder(brandfolder_id:str):
        return f"{BASE_URL}/brandfolders/{brandfolder_id}/assets"

    def get_attachments_in_brandfolder(brandfolder_id:str):
        return f"{BASE_URL}/brandfolders/{brandfolder_id}/attachments"
    
    def get_sections_in_brandfolders(brandfolder_id:str):
        return f"{BASE_URL}/brandfolders/{brandfolder_id}/sections"
    
    def get_assets_in_section(section_id:str):
        return f"{BASE_URL}/sections/{section_id}/assets"

    def get_attachments_in_section(section_id:str):
        return f"{BASE_URL}/sections/{section_id}/attachments"
    
    def get_assets_custom_values(asset_id:str):
        return f"{BASE_URL}/assets/{asset_id}/custom_field_values"
    
    def get_attachments_in_asset(asset_id:str):
        return f"{BASE_URL}/assets/{asset_id}/attachments"
    
    def get_collection_in_brandfolders(brandfolder_id:str):
        return f"{BASE_URL}/brandfolders/{brandfolder_id}/collections"
    
    def get_assets_in_collection(collection_id:str):
        return f"{BASE_URL}/collections/{collection_id}/assets"
    
    def get_attachments_in_collection(collection_id:str):
        return f"{BASE_URL}/collections/{collection_id}/attachments"

@dataclass
class BrandfolderParams:
    page:int=None
    per:int=None
    fields:str=None
    sort_by:str=None
    order:str=None
    include:str=None
    search:str=None

def validate_params(params:BrandfolderParams) -> dict:
    if params is not None:
        final_params_dict = {}
        params_name_list = [field.name for field in fields(params)]
        for param_name in params_name_list:
            param_value = getattr(params,param_name)
            if param_value is not None:
                final_params_dict.update({param_name:param_value})
        
        return final_params_dict

class BrandfolderApi:
    
    def __init__(self,api_key):
        self.api_key=api_key

    def call(self,method:str,url:str,params:BrandfolderParams=None):
        validated_params = validate_params(params)
        headers = {"Authorization":f"Bearer {self.api_key}"}
        response:requests.Response = getattr(requests,method)(url=url,params=validated_params,headers=headers)
        if response.status_code==200:
            data = response.json()
            return data
        else:
            response = {"errorCode":response.status_code,"content":response.content,"data":[],"meta":{"next_page":None}}
            print(response,flush=True)
            return response
        

class BrandfolderSearh:
    relative_expr = {
                        "minute":"m",
                        "hour":"h",
                        "day":"d",
                        "month":"M",
                        "year":"y"
                    }
    default_date_str_format = "%Y-%m-%d"

    @staticmethod
    def last_n_minutes(minutes):
        query_str = f"updated_at:>now-{minutes}{BrandfolderSearh.relative_expr['minute']}"
        return quote_plus(query_str)

    @staticmethod
    def last_n_hours(hours):
        query_str = f"updated_at:>now-{hours}{BrandfolderSearh.relative_expr['hour']}"
        return quote_plus(query_str)
    
    @staticmethod
    def last_n_days(days):
        query_str = f"updated_at:>now-{days}{BrandfolderSearh.relative_expr['day']}"
        return quote_plus(query_str)
    
    @staticmethod
    def last_n_months(months):
        query_str = f"updated_at:>now-{months}{BrandfolderSearh.relative_expr['month']}"
        return quote_plus(query_str)

    @staticmethod
    def last_n_years(years):
        query_str = f"updated_at:>now-{years}{BrandfolderSearh.relative_expr['year']}"
        return quote_plus(query_str)

    @staticmethod
    def utc_created_on(created_at:str):
        date_obj = parse(created_at)
        date_obj_str = date_obj.strftime(BrandfolderSearh.default_date_str_format)
        query_str = f"created_at:{date_obj_str}"
        return quote_plus(query_str)
    
    @staticmethod
    def utc_updated_on(updated_at:str):
        date_obj = parse(updated_at)
        date_obj_str = date_obj.strftime(BrandfolderSearh.default_date_str_format)
        query_str = f"updated_at:{date_obj_str}"
        return quote_plus(query_str)
    

class BranfolderApiJob:
    def __init__(self,api_key,params:BrandfolderParams,uri:str):
        self.api_key=api_key
        self.uri=uri
        self.params=params

    def get(self):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            self.params.order = 'DESC'
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['next_page'] is None:
                loop_status = False
            records = data['data']
            for record in records:
                final_data.append(record)
                page += 1
        
        return final_data
    

class BranfolderApiJobV2:
    def __init__(self,api_key,params:BrandfolderParams,uri:str):
        self.api_key=api_key
        self.uri=uri
        self.params=params

    def get_last_n_min(self,minutes):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            self.params.order = 'DESC'
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['next_page'] is None:
                loop_status = False
            records = data['data']
            for record in records:
                if ValidateTimelineUTC.last_n_minutes(minutes,record['attributes']['updated_at']) is True:
                    final_data.append(record)
                    page += 1
                else:
                    loop_status = False
        
        return final_data

    def get_last_n_hrs(self,hours):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            self.params.order = 'DESC'
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['next_page'] is None:
                loop_status = False
            records = data['data']
            for record in records:
                if ValidateTimelineUTC.last_n_hours(hours,record['attributes']['updated_at']) is True:
                    final_data.append(record)
                    page += 1
                else:
                    loop_status = False
        
        return final_data
    
    def get_this_day(self):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            self.params.order = 'DESC'
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['next_page'] is None:
                loop_status = False
            records = data['data']
            for record in records:
                if ValidateTimelineUTC.this_day(record['attributes']['updated_at']) is True:
                    final_data.append(record)
                    page += 1
                else:
                    loop_status = False

    def get_last_day(self):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            self.params.order = 'DESC'
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['next_page'] is None:
                loop_status = False
            records = data['data']
            for record in records:
                if ValidateTimelineUTC.last_n_days(1,record['attributes']['updated_at']) is True:
                    final_data.append(record)
                    page += 1
                else:
                    loop_status = False
        
        return final_data

    def get_all(self):
        loop_status = True
        page = 1
        final_data = []
        while loop_status is True:
            self.params.page = page
            print(self.params.page,flush=True)
            api_obj = BrandfolderApi(self.api_key)
            data = api_obj.call(method='get',url=self.uri,params=self.params)
            if data['meta']['current_page'] == data['meta']['total_pages']:
                loop_status = False
            records = data['data']
            for record in records:
                final_data.append(record)
                page += 1
        
        return final_data
    
@dataclass
class BrandfolderApiConfig:
    api_key:str
    uri:str=None
    search_query:str=None
    params:BrandfolderParams=None
    select_table_last_hours=None


def process_included_data(included_data:list):
    data = {}
    for record in included_data:
            if record['type'] in data.keys():
                data[record['type']].update({record['id']:record})
            else:
                data.update({record['type']:{}})
                data[record['type']].update({record['id']:record})
    return data