import json
from popsockets_etl.modules.transformation.generic import remove_unicode

class Map:

    @staticmethod
    def product(src_dict:dict):
        output_dict = {}

        try:
            output_dict['id'] = src_dict['result']['request_parameters']['id']
        except:
            output_dict['id'] = None
        try:
            output_dict['app_name'] = src_dict['result']['request_parameters']['appName']
        except:
            output_dict['app_name'] = None
        try:
            output_dict['batch_id'] = src_dict['result']['request_parameters']['batchId']
        except:
            output_dict['batch_id'] = None
        try:
            output_dict['amazon_domain'] = src_dict['result']['request_parameters']['amazon_domain']
        except:
            output_dict['amazon_domain'] = None
        try:
            output_dict['asin'] = src_dict['result']['request_parameters']['asin']
        except:
            output_dict['asin'] = None
        try:
            output_dict['created_at'] = src_dict['result']['request_metadata']['created_at']
        except:
            output_dict['created_at'] = None
        try:
            output_dict['processed_at'] = src_dict['result']['request_metadata']['processed_at']
        except:
            output_dict['processed_at'] = None
        try:
            output_dict['product_title'] = remove_unicode(src_dict['result']['product']['title'])
        except:
            output_dict['product_title'] = None
        try:
            output_dict['product_search_alias_title'] = src_dict['result']['product']['search_alias']['title']
        except:
            output_dict['product_search_alias_title'] = None
        try:
            output_dict['product_search_alias_value'] = src_dict['result']['product']['search_alias']['value']
        except:
            output_dict['product_search_alias_value'] = None
        try:
            output_dict['product_keywords'] = src_dict['result']['product']['keywords']
        except:
            output_dict['product_keywords'] = None
        try:
            output_dict['product_keywords_list'] = json.dumps(remove_unicode(src_dict['result']['product']['keywords_list']))
        except:
            output_dict['product_keywords_list'] = None
        try:
            output_dict['product_link'] = src_dict['result']['product']['link']
        except:
            output_dict['product_link'] = None
        try:
            output_dict['product_brand'] = src_dict['result']['product']['brand']
        except:
            output_dict['product_brand'] = None
        try:
            output_dict['product_variants'] = json.dumps(remove_unicode(src_dict['result']['product']['variants']))
        except:
            output_dict['product_variants'] = None
        try:
            output_dict['product_sell_on_amazon'] = src_dict['result']['product']['sell_on_amazon']
        except:
            output_dict['product_sell_on_amazon'] = None
        try:
            output_dict['product_proposition_65_warning'] = src_dict['result']['product']['proposition_65_warning']
        except:
            output_dict['product_proposition_65_warning'] = None
        try:
            output_dict['product_variant_asins_flat'] = src_dict['result']['product']['variant_asins_flat']
        except:
            output_dict['product_variant_asins_flat'] = None
        try:
            output_dict['product_has_size_guide'] = src_dict['result']['product']['has_size_guide']
        except:
            output_dict['product_has_size_guide'] = None
        try:
            output_dict['product_categories'] = json.dumps(remove_unicode(src_dict['result']['product']['categories']))
        except:
            output_dict['product_categories'] = None
        try:
            output_dict['product_a_plus_content_has_a_plus_content'] = src_dict['result']['product']['a_plus_content']['has_a_plus_content']
        except:
            output_dict['product_a_plus_content_has_a_plus_content'] = None
        try:
            output_dict['product_a_plus_content_has_brand_story'] = src_dict['result']['product']['a_plus_content']['has_brand_story']
        except:
            output_dict['product_a_plus_content_has_brand_story'] = None
        try:
            output_dict['product_a_plus_content_third_party'] = src_dict['result']['product']['a_plus_content']['third_party']
        except:
            output_dict['product_a_plus_content_third_party'] = None
###############################################################################################################
        try:
            output_dict['product_description'] = remove_unicode(src_dict['result']['product']['description'])
        except:
            output_dict['product_description'] = None
        try:
            output_dict['product_sub_title_text'] = src_dict['result']['product']['sub_title']['text']
        except:
            output_dict['product_sub_title_text'] = None
        try:
            output_dict['product_sub_title_link'] = src_dict['result']['product']['sub_title']['link']
        except:
            output_dict['product_sub_title_link'] = None
        try:
            output_dict['product_rating'] = src_dict['result']['product']['rating']
        except:
            output_dict['product_rating'] = None
        try:
            output_dict['rating_breakdown_five_star_percentage'] = src_dict['result']['product']['rating_breakdown']['five_star']['percentage']
        except:
            output_dict['rating_breakdown_five_star_percentage'] = None
        try:
            output_dict['rating_breakdown_five_star_count'] = src_dict['result']['product']['rating_breakdown']['five_star']['count']
        except:
            output_dict['rating_breakdown_five_star_count'] = None
        try:
            output_dict['rating_breakdown_four_star_percentage'] = src_dict['result']['product']['rating_breakdown']['four_star']['percentage']
        except:
            output_dict['rating_breakdown_four_star_percentage'] = None
        try:
            output_dict['rating_breakdown_four_star_count'] = src_dict['result']['product']['rating_breakdown']['four_star']['count']
        except:
            output_dict['rating_breakdown_four_star_count'] = None
        try:
            output_dict['rating_breakdown_three_star_percentage'] = src_dict['result']['product']['rating_breakdown']['three_star']['percentage']
        except:
            output_dict['rating_breakdown_three_star_percentage'] = None
        try:
            output_dict['rating_breakdown_three_star_count'] = src_dict['result']['product']['rating_breakdown']['three_star']['count']
        except:
            output_dict['rating_breakdown_three_star_count'] = None
        try:
            output_dict['rating_breakdown_two_star_percentage'] = src_dict['result']['product']['rating_breakdown']['two_star']['percentage']
        except:
            output_dict['rating_breakdown_two_star_percentage'] = None
        try:
            output_dict['rating_breakdown_two_star_count'] = src_dict['result']['product']['rating_breakdown']['two_star']['count']
        except:
            output_dict['rating_breakdown_two_star_count'] = None
        try:
            output_dict['rating_breakdown_one_star_percentage'] = src_dict['result']['product']['rating_breakdown']['one_star']['percentage']
        except:
            output_dict['rating_breakdown_one_star_percentage'] = None
        try:
            output_dict['rating_breakdown_one_star_count'] = src_dict['result']['product']['rating_breakdown']['one_star']['count']
        except:
            output_dict['rating_breakdown_one_star_count'] = None
        try:
            output_dict['product_ratings_total'] = src_dict['result']['product']['ratings_total']
        except:
            output_dict['product_ratings_total'] = None
        try:
            output_dict['product_main_image_link'] = src_dict['result']['product']['main_image']['link']
        except:
            output_dict['product_main_image_link'] = None
        try:
            output_dict['product_images'] = json.dumps(remove_unicode(src_dict['result']['product']['images']))
        except:
            output_dict['product_images'] = None
        try:
            output_dict['product_images_count'] = src_dict['result']['product']['images_count']
        except:
            output_dict['product_images_count'] = None
        try:
            output_dict['product_images_flat'] = src_dict['result']['product']['images_flat']
        except:
            output_dict['product_images_flat'] = None
        try:
            output_dict['product_videos'] = json.dumps(remove_unicode(src_dict['result']['product']['videos']))
        except:
            output_dict['product_videos'] = None
        try:
            output_dict['product_videos_count'] = src_dict['result']['product']['videos_count']
        except:
            output_dict['product_videos_count'] = None
        try:
            output_dict['product_videos_flat'] = src_dict['result']['product']['videos_flat']
        except:
            output_dict['product_videos_flat'] = None
        try:
            output_dict['product_has_360_view'] = src_dict['result']['product']['has_360_view']
        except:
            output_dict['product_has_360_view'] = None
        try:
            output_dict['product_is_bundle'] = src_dict['result']['product']['is_bundle']
        except:
            output_dict['product_is_bundle'] = None
        try:
            output_dict['product_feature_bullets'] = json.dumps(remove_unicode(src_dict['result']['product']['feature_bullets']))
        except:
            output_dict['product_feature_bullets'] = None
        try:
            output_dict['product_feature_bullets_count'] = src_dict['result']['product']['feature_bullets_count']
        except:
            output_dict['product_feature_bullets_count'] = None
        try:
            output_dict['product_feature_bullets_flat'] = src_dict['result']['product']['feature_bullets_flat']
        except:
            output_dict['product_feature_bullets_flat'] = None
        try:
            output_dict['product_top_reviews'] = json.dumps(remove_unicode(src_dict['result']['product']['top_reviews']))
        except:
            output_dict['product_top_reviews'] = None
        try:
            output_dict['product_buybox_winner_is_prime'] = src_dict['result']['product']['buybox_winner']['is_prime']
        except:
            output_dict['product_buybox_winner_is_prime'] = None
        try:
            output_dict['product_buybox_winner_is_prime_exclusive_deal'] = src_dict['result']['product']['buybox_winner']['is_prime_exclusive_deal']
        except:
            output_dict['product_buybox_winner_is_prime_exclusive_deal'] = None
        try:
            output_dict['product_buybox_winner_is_amazon_fresh'] = src_dict['result']['product']['buybox_winner']['is_amazon_fresh']
        except:
            output_dict['product_buybox_winner_is_amazon_fresh'] = None
        try:
            output_dict['product_buybox_winner_condition_is_new'] = src_dict['result']['product']['buybox_winner']['condition']['is_new']
        except:
            output_dict['product_buybox_winner_condition_is_new'] = None
        try:
            output_dict['product_buybox_winner_availability_type'] = src_dict['result']['product']['buybox_winner']['availability']['type']
        except:
            output_dict['product_buybox_winner_availability_type'] = None
        try:
            output_dict['product_buybox_winner_availability_raw'] = src_dict['result']['product']['buybox_winner']['availability']['raw']
        except:
            output_dict['product_buybox_winner_availability_raw'] = None
        try:
            output_dict['product_specifications'] = json.dumps(remove_unicode(src_dict['result']['product']['specifications']))
        except:
            output_dict['product_specifications'] = None
        try:
            output_dict['product_specifications_flat'] = src_dict['result']['product']['specifications_flat']
        except:
            output_dict['product_specifications_flat'] = None
        try:
            output_dict['product_material'] = src_dict['result']['product']['material']
        except:
            output_dict['product_material'] = None
        try:
            output_dict['product_weight'] = src_dict['result']['product']['weight']
        except:
            output_dict['product_weight'] = None
        try:
            output_dict['product_first_available_raw'] = src_dict['result']['product']['first_available']['raw']
        except:
            output_dict['product_first_available_raw'] = None
        try:
            output_dict['product_first_available_utc'] = src_dict['result']['product']['first_available']['utc']
        except:
            output_dict['product_first_available_utc'] = None
        try:
            output_dict['product_dimensions'] = src_dict['result']['product']['dimensions']
        except:
            output_dict['product_dimensions'] = None
        try:
            output_dict['product_model_number'] = src_dict['result']['product']['model_number']
        except:
            output_dict['product_model_number'] = None
        try:
            output_dict['product_brand_store_id'] = src_dict['result']['brand_store']['id']
        except:
            output_dict['product_brand_store_id'] = None
        try:
            output_dict['product_brand_store_link'] = src_dict['result']['brand_store']['link']
        except:
            output_dict['product_brand_store_link'] = None

        return output_dict
    
    @staticmethod
    def asin_data_validation_data(src_dict:dict):
        output_dict = {}

        try:
            output_dict['vendor_sku'] = src_dict['Vendor SKU']
        except:
            output_dict['vendor_sku'] = None
        try:
            output_dict['seo_cheat_category'] = src_dict['SEO Cheat Category']
        except:
            output_dict['seo_cheat_category'] = None
        try:
            output_dict['title'] = src_dict['Title']
        except:
            output_dict['title'] = None
        try:
            output_dict['upc'] = src_dict['UPC']
        except:
            output_dict['upc'] = None
        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['list_price'] = src_dict['List Price']
        except:
            output_dict['list_price'] = None
        try:
            output_dict['cost_price'] = src_dict['Cost Price']
        except:
            output_dict['cost_price'] = None
        try:
            output_dict['bullet_point_1'] = src_dict['Bullet Point 1']
        except:
            output_dict['bullet_point_1'] = None
        try:
            output_dict['bullet_point_2'] = src_dict['Bullet Point 2']
        except:
            output_dict['bullet_point_2'] = None
        try:
            output_dict['bullet_point_3'] = src_dict['Bullet Point 3']
        except:
            output_dict['bullet_point_3'] = None
        try:
            output_dict['bullet_point_4'] = src_dict['Bullet Point 4']
        except:
            output_dict['bullet_point_4'] = None
        try:
            output_dict['bullet_point_5'] = src_dict['Bullet Point 5']
        except:
            output_dict['bullet_point_5'] = None
        try:
            output_dict['backend_keywords'] = src_dict['Backend Keywords']
        except:
            output_dict['backend_keywords'] = None
        try:
            output_dict['special_features_1'] = src_dict['Special Features 1']
        except:
            output_dict['special_features_1'] = None
        try:
            output_dict['special_features_2'] = src_dict['Special Features 2']
        except:
            output_dict['special_features_2'] = None
        try:
            output_dict['special_features_3'] = src_dict['Special Features 3']
        except:
            output_dict['special_features_3'] = None
        try:
            output_dict['special_features_4'] = src_dict['Special Features 4']
        except:
            output_dict['special_features_4'] = None
        try:
            output_dict['special_features_5'] = src_dict['Special Features 5']
        except:
            output_dict['special_features_5'] = None
        try:
            output_dict['product_description'] = src_dict['Product Description']
        except:
            output_dict['product_description'] = None
        try:
            output_dict['color'] = src_dict['Color']
        except:
            output_dict['color'] = None
        try:
            output_dict['mounting_type'] = src_dict['Mounting Type']
        except:
            output_dict['mounting_type'] = None
        try:
            output_dict['included_components'] = src_dict['Included Components']
        except:
            output_dict['included_components'] = None
        try:
            output_dict['grip_type'] = src_dict['Grip Type']
        except:
            output_dict['grip_type'] = None
        try:
            output_dict['variation_group_number'] = src_dict['Variation Group Number']
        except:
            output_dict['variation_group_number'] = None
        try:
            output_dict['variation_group'] = src_dict['Variation Group']
        except:
            output_dict['variation_group'] = None
        try:
            output_dict['child_sku'] = src_dict['Child SKU']
        except:
            output_dict['child_sku'] = None
        try:
            output_dict['child_asin'] = src_dict['Child ASIN']
        except:
            output_dict['child_asin'] = None
        try:
            output_dict['variations_title'] = src_dict['Variations Title']
        except:
            output_dict['variations_title'] = None
        try:
            output_dict['theme'] = src_dict['Theme']
        except:
            output_dict['theme'] = None
        try:
            output_dict['style_name'] = src_dict['STYLE_NAME']
        except:
            output_dict['style_name'] = None
        try:
            output_dict['color_name'] = src_dict['COLOR_NAME']
        except:
            output_dict['color_name'] = None

        return output_dict