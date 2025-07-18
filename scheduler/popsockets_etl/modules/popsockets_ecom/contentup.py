import json
import re
from pydantic import BaseModel
from typing import Union, List
import inspect
import ast
import datetime as dt
import uuid
from sqlalchemy import text
import pandas as pd

def get_class_method_order(cls):
    source = inspect.getsource(cls)
    tree = ast.parse(source)

    method_names = []

    class_node = tree.body[0]
    if isinstance(class_node, ast.ClassDef):
        for node in class_node.body:
            if isinstance(node, ast.FunctionDef):
                method_names.append(node.name)

    return method_names

def verify_variations_group(parent_asin:str,live_data_child_asins:Union[List[str],None],validation_data_df:pd.DataFrame):
    if live_data_child_asins is not None:
        asin_list = str(live_data_child_asins).split(sep=",")
        asin_list.append(parent_asin)
        filtered_df = validation_data_df[validation_data_df['asin'].isin(asin_list)]
        filtered_dict = filtered_df.to_dict(orient="records")
        if not filtered_dict:
            return None 
        reference_value = filtered_dict[0]['variation_group_number']
        # all_equals = all(record['variation_group_number']==filtered_dict[0] for record in filtered_dict)
        all_equals = all(record['variation_group_number'] == reference_value for record in filtered_dict)
        return all_equals

class ComparisionOutputSchema(BaseModel):
    comparision_label: str
    live_field_name: Union[str,None]
    live_field_value: Union[str,int,float,bool] | None
    validation_data_field_name: str|None
    validation_data_field_value: Union[str,int,float,bool,None]


def normalize_for_comparison(text: Union[str,None]) -> str|None:
    """
    Normalize text for comparison with enhanced rules:
    1. Convert to lowercase
    2. Normalize various quote types
    3. Remove ALL punctuation
    4. Normalize whitespace
    5. Handle common substitutions
    """
    if text is not None:
        if not isinstance(text, str):
            return ""
        text = text.lower()
        quote_chars = ['"', '"', '"', '‖', '″', '\'', ''', ''', '`', '´']
        for quote in quote_chars:
            text = text.replace(quote, ' ')
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        substitutions = {
            'w/': 'with',
            'w/o': 'without',
            '&': 'and',
            '+': 'plus',
            '@': 'at',
            '=': 'equals',
            'vs': 'versus',
            'vs.': 'versus',
        }
        for old, new in substitutions.items():
            text = re.sub(r'\b' + re.escape(old) + r'\b', new, text, flags=re.IGNORECASE)
        return text.strip()

class ContentUpEvaluation:
    def __init__(self,validation_data_df:pd.DataFrame,asin_collection_api_data:dict,master_validation_data:dict):
        self.validation_data_df=validation_data_df
        self.asin_collection_api_data=asin_collection_api_data
        self.master_validation_data=master_validation_data
        self.final_result=[]
        self.final_score=0

    def title_validation(self,score=10):
        COMPARISION_LABEL="Is Title Correct?"
        LIVE_FIELD_NAME="product_title"
        VALIDATION_DATA_FIELD_NAME="title"
        live_title = self.asin_collection_api_data[LIVE_FIELD_NAME]
        validation_data_title = self.master_validation_data['title']
        result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_title),
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=validation_data_title
                                            )
        result_dict = result.model_dump()
        if live_title is not None:
            normalized_live_title = normalize_for_comparison(VALIDATION_DATA_FIELD_NAME)
            normalized_validation_data_title = normalize_for_comparison(VALIDATION_DATA_FIELD_NAME)
            if normalized_live_title==normalized_validation_data_title:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
            else:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def bullet_points_exists_5(self,score=5):
        COMPARISION_LABEL="Do we have 5 bullet points?"
        LIVE_FIELD_NAME="product_feature_bullets_count"
        VALIDATION_DATA_FIELD_NAME=None
        live_bullet_points_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_bullet_points_count),
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=None
                                            )
        result_dict = result.model_dump()
        if live_bullet_points_count is not None:
            if live_bullet_points_count >= 5:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
            else:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    
    def match_bullet_points(self,score=2):
        COMPARISION_LABEL="Are bullet points correct?"
        LIVE_FIELD_NAME =   "product_feature_bullets"
        VALIDATION_DATA_FIELD_NAME="bullet_point_1,bullet_point_2,bullet_point_3,bullet_point_4,bullet_point_5"
        live_bullet_points = self.asin_collection_api_data[LIVE_FIELD_NAME]
        if live_bullet_points is not None:
            normalized_live_bullet_points_list = [normalize_for_comparison(bp) for bp in json.loads(live_bullet_points)]
            validaton_data_bullet_point_list = [self.master_validation_data['bullet_point_1'],self.master_validation_data['bullet_point_2'],self.master_validation_data['bullet_point_3'],self.master_validation_data['bullet_point_4'],self.master_validation_data['bullet_point_5']]
            normalized_validaton_data_bullet_point_list = [normalize_for_comparison(bp) for bp in validaton_data_bullet_point_list]
            bullet_point_list_comparision = normalized_live_bullet_points_list[:len(normalized_validaton_data_bullet_point_list)] == normalized_validaton_data_bullet_point_list
            result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_bullet_points),
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=json.dumps(validaton_data_bullet_point_list)
                                            )
            result_dict = result.model_dump()
            if bullet_point_list_comparision is True:
                    self.final_score += score
                    result_dict.update({"result":"success","score":str(score)})
            else:
                result_dict.update({"result":"failed","score":0})
            
            self.final_result.append(result_dict)
            return True
        else:
            return False

    def is_description_exists(self,score=4):
        COMPARISION_LABEL="Is Description exists?"
        LIVE_FIELD_NAME="product_description"
        live_description = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_description),
                                            validation_data_field_name=None,
                                            validation_data_field_value=None
                                            )
        result_dict = result.model_dump()
        if live_description is not None:
            
            if live_description is None:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
                return False
            else:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
                return True
        else:
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return False

    def is_description_correct(self,score=2):
        COMPARISION_LABEL="Is Description Correct?"
        LIVE_FIELD_NAME="product_description"
        VALIDATION_DATA_FIELD_NAME="product_description"
        live_description = self.asin_collection_api_data[LIVE_FIELD_NAME]
        if live_description is not None:
            validation_data_description = self.master_validation_data[VALIDATION_DATA_FIELD_NAME]
            result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_description),
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=validation_data_description
                                            )
            result_dict = result.model_dump()
            if normalize_for_comparison(live_description)==normalize_for_comparison(validation_data_description):
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
            else:
                result_dict.update({"result":"failed","score":0})
            
            self.final_result.append(result_dict)
            return True
        else:
            return False

    def backend_keywords_exists(self,score=5,check=False):
        COMPARISION_LABEL="Do Backends Keywords Exists?"
        VALIDATION_DATA_FIELD_NAME="backend_keywords"
        validation_data_backend_keywords = self.master_validation_data[VALIDATION_DATA_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=None,
                                        live_field_value=None,
                                        validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                        validation_data_field_value=validation_data_backend_keywords
                                        )
        result_dict = result.model_dump()
        if validation_data_backend_keywords is None:
            if check is False:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
                return False
            else:
                return False
        else:
            if check is False:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
                return True
            else:
                return True

    def are_backend_keywords_correct(self,score=5):
        """Placeholder Function"""
        COMPARISION_LABEL="Are Backend Keywords Correct?"
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=None,
                                        live_field_value=None,
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        eval = self.backend_keywords_exists(check=True)
        if eval is True:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def does_variations_exists(self,score=10):
        COMPARISION_LABEL="Does Variations Exists?"
        LIVE_FIELD_NAME="product_variants"
        live_variations = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_variations),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if live_variations is not None:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def are_variations_correct(self,score=10):
        COMPARISION_LABEL="Are Variations Correct?"
        LIVE_FIELD_NAME="product_variant_asins_flat"
        parent_asin = self.asin_collection_api_data['asin']
        live_variations = self.asin_collection_api_data[LIVE_FIELD_NAME]
        variation_group_match = verify_variations_group(parent_asin=parent_asin,live_data_child_asins=live_variations,validation_data_df=self.validation_data_df)
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_variations),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if live_variations is not None and variation_group_match is not None:
            if variation_group_match is True:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
                return True
            else:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
                return False
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def does_a_plus_content_exists(self,score=4,check=False):
        COMPARISION_LABEL="Does A+ Content Exists?"
        LIVE_FIELD_NAME="product_a_plus_content_has_a_plus_content"
        live_has_a_plus_content = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_has_a_plus_content),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if live_has_a_plus_content is True:
            if check is False:
                self.final_score += score
                result_dict.update({"result":"success","score":str(score)})
                self.final_result.append(result_dict)
                return True
            else:
                return True
        else:
            if check is False:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
                return False
            else:
                return False

    def is_a_plus_content_correct(self,score=5):
        """Placeholder Function"""
        COMPARISION_LABEL="Is A+ Content Correct?"
        LIVE_FIELD_NAME="product_a_plus_content_has_a_plus_content"
        live_has_a_plus_content = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_has_a_plus_content),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if self.does_a_plus_content_exists(check=True) is True:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def have_six_images(self,score=5,check=False):
        COMPARISION_LABEL="Do we have six images?"
        LIVE_FIELD_NAME="product_images_count"
        live_images_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_images_count),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if live_images_count is not None:
            if live_images_count >= 6:
                if check is False:
                    self.final_score += score
                    result_dict.update({"result":"success","score":str(score)})
                    self.final_result.append(result_dict)
                    return True
                else:
                    return True
            else:
                if check is False:
                    result_dict.update({"result":"failed","score":0})
                    self.final_result.append(result_dict)
                    return False
                else:
                    return False    
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def are_images_order_correct(self,score=10):
        """Placeholder Function"""
        COMPARISION_LABEL="Are Images Order Correct?"
        LIVE_FIELD_NAME="product_images_count"
        live_images_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_images_count),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if self.have_six_images(check=True) is True:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def is_image_quality_correct(self,score=4):
        """Placeholder Function"""
        COMPARISION_LABEL="Is Images Quality Correct?"
        LIVE_FIELD_NAME="product_images_count"
        live_images_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_images_count),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if self.have_six_images(check=True) is True:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def video_exists(self,score=5,check=False):
        COMPARISION_LABEL="Does Video Exists?"
        LIVE_FIELD_NAME="product_videos_count"
        live_videos_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_videos_count),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if live_videos_count is not None:
            if live_videos_count > 0:
                if check is False:
                    self.final_score += score
                    result_dict.update({"result":"success","score":str(score)})
                    self.final_result.append(result_dict)
                    return True
                else:
                    return True
            else:
                if check is False:
                    result_dict.update({"result":"failed","score":0})
                    self.final_result.append(result_dict)
                    return False
                else:
                    return False
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def is_video_correct(self,score=5):
        """Placeholder Function"""
        COMPARISION_LABEL="Is Video Correct?"
        LIVE_FIELD_NAME="product_videos_count"
        live_videos_count = self.asin_collection_api_data[LIVE_FIELD_NAME]
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=str(live_videos_count),
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        if self.video_exists(check=True) is True:
            self.final_score += score
            result_dict.update({"result":"success","score":str(score)})
            self.final_result.append(result_dict)
            return True
        else:
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

    def idq_score(self,score=4):
        """Placeholder Function"""
        COMPARISION_LABEL="IDQ Score"
        LIVE_FIELD_NAME=None
        result = ComparisionOutputSchema(
                                        comparision_label=COMPARISION_LABEL,
                                        live_field_name=str(LIVE_FIELD_NAME),
                                        live_field_value=None,
                                        validation_data_field_name=None,
                                        validation_data_field_value=None
                                        )
        result_dict = result.model_dump()
        # Placeholder: Default result is True
        self.final_score += score
        result_dict.update({"result":"success","score":str(score)})
        self.final_result.append(result_dict)
        return True

    def are_all_special_fields_correct(self,score=5):
        COMPARISION_LABEL="Are All Special Fields Correct?"
        LIVE_FIELD_NAME="product_specifications"
        VALIDATION_DATA_FIELD_NAME="grip_type"
        live_product_specifications_json = json.loads(self.asin_collection_api_data[LIVE_FIELD_NAME])
        validation_data_grip_type = self.master_validation_data['grip_type']
        if live_product_specifications_json is not None:
            live_grip_type = [record['value'] for record in live_product_specifications_json if record['name']=='Grip Type']
            grip_type_exists = False
            if len(live_grip_type) > 0:
                grip_type_exists = True
                live_grip_type = live_grip_type[0]
            else:
                live_grip_type = None
            result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=str(live_grip_type),
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=validation_data_grip_type
                                            )
            result_dict = result.model_dump()
            if grip_type_exists:
                if normalize_for_comparison(live_grip_type)==normalize_for_comparison(validation_data_grip_type):
                    self.final_score += score
                    result_dict.update({"result":"success","score":str(score)})
                    self.final_result.append(result_dict)
                    return True
            else:
                result_dict.update({"result":"failed","score":0})
                self.final_result.append(result_dict)
                return False
        else:
            result = ComparisionOutputSchema(
                                            comparision_label=COMPARISION_LABEL,
                                            live_field_name=str(LIVE_FIELD_NAME),
                                            live_field_value=None,
                                            validation_data_field_name=str(VALIDATION_DATA_FIELD_NAME),
                                            validation_data_field_value=validation_data_grip_type
                                            )
            result_dict = result.model_dump()
            result_dict.update({"result":"failed","score":0})
            self.final_result.append(result_dict)
            return False

def evaluate(validation_data_df:pd.DataFrame,asin_collection_api_data:dict,master_validation_data:dict):
    comparision_obj = ContentUpEvaluation(validation_data_df,asin_collection_api_data, master_validation_data)

    class_methods = get_class_method_order(ContentUpEvaluation)

    # print(class_methods)
    for method in class_methods:
        # print(f"Evaluation Method: {method}",flush=True,end="\r")
        # Skip private methods like __init__ etc.
        if not method.startswith("__"):
            if method=="title_validation":
                result = getattr(comparision_obj, method)()
                if result is False:
                    break
                else:
                    continue
            else:
                getattr(comparision_obj, method)()
    return {
        "asin": master_validation_data['asin'],
        # "amazon_domain": asin_collection_api_data['amazon_domain'],
        "score": comparision_obj.final_score,
        "results": comparision_obj.final_result
        }