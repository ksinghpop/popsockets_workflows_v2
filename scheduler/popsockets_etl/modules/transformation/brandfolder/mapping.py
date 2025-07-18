from popsockets_etl.modules.transformation.json_flattening import flatten_json
import json

class Map:

    @staticmethod
    def organisation(src_dict:dict):
        final_dict = {}
        
        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_name'] = src_dict['attributes']['name']
        except:
            final_dict['attributes_name'] = None
        try:
            final_dict['attributes_slug'] = src_dict['attributes']['slug']
        except:
            final_dict['attributes_slug'] = None
        try:
            final_dict['attributes_tagline'] = src_dict['attributes']['tagline']
        except:
            final_dict['attributes_tagline'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None

        return final_dict
    

    def brandfolder(src_dict:dict,organisation_id:str):
        final_dict = {}
        
        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_name'] = src_dict['attributes']['name']
        except:
            final_dict['attributes_name'] = None
        try:
            final_dict['attributes_tagline'] = src_dict['attributes']['tagline']
        except:
            final_dict['attributes_tagline'] = None
        try:
            final_dict['attributes_privacy'] = src_dict['attributes']['privacy']
        except:
            final_dict['attributes_privacy'] = None
        try:
            final_dict['attributes_slug'] = src_dict['attributes']['slug']
        except:
            final_dict['attributes_slug'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None
        try:
            final_dict['organisation_id'] = organisation_id
        except:
            final_dict['organisation_id'] = None

        return final_dict
    
    def section(src_dict:dict,brandfolder_id:str):
        final_dict = {}
        
        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_name'] = src_dict['attributes']['name']
        except:
            final_dict['attributes_name'] = None
        try:
            final_dict['attributes_default_asset_type'] = src_dict['attributes']['default_asset_type']
        except:
            final_dict['attributes_default_asset_type'] = None
        try:
            final_dict['attributes_position'] = src_dict['attributes']['position']
        except:
            final_dict['attributes_position'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None
        try:
            final_dict['brandfolder_id'] = brandfolder_id
        except:
            final_dict['brandfolder_id'] = None

        return final_dict
    
    def collection(src_dict:dict,brandfolder_id:str):
        final_dict = {}

        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_name'] = src_dict['attributes']['name']
        except:
            final_dict['attributes_name'] = None
        try:
            final_dict['attributes_slug'] = src_dict['attributes']['slug']
        except:
            final_dict['attributes_slug'] = None
        try:
            final_dict['attributes_tagline'] = src_dict['attributes']['tagline']
        except:
            final_dict['attributes_tagline'] = None
        try:
            final_dict['attributes_public'] = src_dict['attributes']['public']
        except:
            final_dict['attributes_public'] = None
        try:
            final_dict['attributes_stealth'] = src_dict['attributes']['stealth']
        except:
            final_dict['attributes_stealth'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None
        try:
            final_dict['attributes_is_workspace'] = src_dict['attributes']['is_workspace']
        except:
            final_dict['attributes_is_workspace'] = None
        try:
            final_dict['brandfolder_id'] = brandfolder_id
        except:
            final_dict['brandfolder_id'] = None

        return final_dict
    
    def asset(src_dict:dict,section_id:str,collection_ids:list,custom_field_values:list,tags:list):
        final_dict = {}

        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_name'] = src_dict['attributes']['name']
        except:
            final_dict['attributes_name'] = None
        try:
            final_dict['attributes_description'] = src_dict['attributes']['description']
        except:
            final_dict['attributes_description'] = None
        try:
            final_dict['attributes_approved'] = src_dict['attributes']['approved']
        except:
            final_dict['attributes_approved'] = None
        try:
            final_dict['attributes_cdn_url'] = src_dict['attributes']['cdn_url']
        except:
            final_dict['attributes_cdn_url'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None
        try:
            final_dict['section_id'] = section_id
        except:
            final_dict['section_id'] = None
        try:
            final_dict['collection_ids'] = json.dumps(collection_ids)
        except:
            final_dict['collection_ids'] = None
        try:
            final_dict['custom_field_values'] =  json.dumps(custom_field_values)
        except:
            final_dict['custom_field_values'] = None
        try:
            final_dict['tags'] =  json.dumps(tags)
        except:
            final_dict['tags'] = None
        
        return final_dict
    
    def attachment(src_dict:str,asset_id:str):
        final_dict = {}

        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_mimetype'] = src_dict['attributes']['mimetype']
        except:
            final_dict['attributes_mimetype'] = None
        try:
            final_dict['attributes_extension'] = src_dict['attributes']['extension']
        except:
            final_dict['attributes_extension'] = None
        try:
            final_dict['attributes_filename'] = src_dict['attributes']['filename']
        except:
            final_dict['attributes_filename'] = None
        try:
            final_dict['attributes_size'] = src_dict['attributes']['size']
        except:
            final_dict['attributes_size'] = None
        try:
            final_dict['attributes_width'] = src_dict['attributes']['width']
        except:
            final_dict['attributes_width'] = None
        try:
            final_dict['attributes_height'] = src_dict['attributes']['height']
        except:
            final_dict['attributes_height'] = None
        try:
            final_dict['attributes_position'] = src_dict['attributes']['position']
        except:
            final_dict['attributes_position'] = None
        try:
            final_dict['attributes_cdn_url'] = src_dict['attributes']['cdn_url']
        except:
            final_dict['attributes_cdn_url'] = None
        try:
            final_dict['attributes_created_at'] = src_dict['attributes']['created_at']
        except:
            final_dict['attributes_created_at'] = None
        try:
            final_dict['attributes_updated_at'] = src_dict['attributes']['updated_at']
        except:
            final_dict['attributes_updated_at'] = None
        try:
            final_dict['asset_id'] = asset_id
        except:
            final_dict['asset_id'] = None

        return final_dict

    def asset_custom_fields(src_dict:str,asset_id:str):
        final_dict = {}

        try:
            final_dict['id'] = src_dict['id']
        except:
            final_dict['id'] = None
        try:
            final_dict['type'] = src_dict['type']
        except:
            final_dict['type'] = None
        try:
            final_dict['attributes_key'] = src_dict['attributes']['key']
        except:
            final_dict['attributes_key'] = None
        try:
            final_dict['attributes_value'] = src_dict['attributes']['value']
        except:
            final_dict['attributes_value'] = None
        try:
            final_dict['asset_id'] = asset_id
        except:
            final_dict['asset_id'] = None

        return final_dict
    
    def asset_collections(asset_id:str,collection_id:str):
        final_dict = {}

        try:
            final_dict['asset_id'] = asset_id
        except:
            final_dict['asset_id'] = None
        try:
            final_dict['collection_id'] = collection_id
        except:
            final_dict['collection_id'] = None

        return final_dict