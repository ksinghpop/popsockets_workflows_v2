from dateutil.parser import parse
import pandas as pd

class Map:
    @staticmethod
    def amz_emea_ca_inventory(src_dict:dict):
        output_dict = {}
        # Mapping Attachment Fields
        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['sourceable_product_oos'] = src_dict['Sourceable Product OOS']
        except:
            output_dict['sourceable_product_oos'] = None
        try:
            output_dict['vendor_confirmation_rate'] = src_dict['Vendor Confirmation Rate']
        except:
            output_dict['vendor_confirmation_rate'] = None
        try:
            output_dict['net_received'] = src_dict['Net Received']
        except:
            output_dict['net_received'] = None
        try:
            output_dict['net_received_units'] = src_dict['Net Received Units']
        except:
            output_dict['net_received_units'] = None
        try:
            output_dict['open_purchase_order_quantity'] = src_dict['Open Purchase Order Quantity']
        except:
            output_dict['open_purchase_order_quantity'] = None
        try:
            output_dict['receive_fill_rate'] = src_dict['Receive Fill Rate %']
        except:
            output_dict['receive_fill_rate'] = None
        try:
            output_dict['overall_vendor_lead_time_days'] = src_dict['Overall Vendor Lead Time (days)']
        except:
            output_dict['overall_vendor_lead_time_days'] = None
        try:
            output_dict['unfilled_customer_ordered_units'] = src_dict['Unfilled Customer Ordered Units']
        except:
            output_dict['unfilled_customer_ordered_units'] = None
        try:
            output_dict['aged_90_days_sellable_inventory'] = src_dict['Aged 90+ Days Sellable Inventory']
        except:
            output_dict['aged_90_days_sellable_inventory'] = None
        try:
            output_dict['aged_90_days_sellable_units'] = src_dict['Aged 90+ Days Sellable Units']
        except:
            output_dict['aged_90_days_sellable_units'] = None
        try:
            output_dict['sellable_on_hand_inventory'] = src_dict['Sellable On Hand Inventory']
        except:
            output_dict['sellable_on_hand_inventory'] = None
        try:
            output_dict['sellable_on_hand_units'] = src_dict['Sellable On Hand Units']
        except:
            output_dict['sellable_on_hand_units'] = None
        try:
            output_dict['unsellable_on_hand_inventory'] = src_dict['Unsellable On Hand Inventory']
        except:
            output_dict['unsellable_on_hand_inventory'] = None
        try:
            output_dict['unsellable_on_hand_units'] = src_dict['Unsellable On Hand Units']
        except:
            output_dict['unsellable_on_hand_units'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None


        return output_dict
    
    @staticmethod
    def amz_emea_ca_sales(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['ordered_revenue'] = src_dict['Ordered revenue']
        except:
            output_dict['ordered_revenue'] = None
        try:
            output_dict['ordered_units'] = src_dict['Ordered units']
        except:
            output_dict['ordered_units'] = None
        try:
            output_dict['shipped_revenue'] = src_dict['Shipped revenue']
        except:
            output_dict['shipped_revenue'] = None
        try:
            output_dict['shipped_cogs'] = src_dict['Shipped COGS']
        except:
            output_dict['shipped_cogs'] = None
        try:
            output_dict['shipped_units'] = src_dict['Shipped units']
        except:
            output_dict['shipped_units'] = None
        try:
            output_dict['customer_returns'] = src_dict['Customer returns']
        except:
            output_dict['customer_returns'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    @staticmethod
    def amz_emea_ca_traffic(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['glance_views'] = src_dict['Glance views']
        except:
            output_dict['glance_views'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    @staticmethod
    def amz_emea_de_inventory(src_dict:dict):
        output_dict = {}
        # Mapping Attachment Fields
        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['sourceable_product_oos'] = src_dict['Sourceable Product OOS']
        except:
            output_dict['sourceable_product_oos'] = None
        try:
            output_dict['vendor_confirmation_rate'] = src_dict['Vendor Confirmation Rate']
        except:
            output_dict['vendor_confirmation_rate'] = None
        try:
            output_dict['net_received'] = src_dict['Net Received']
        except:
            output_dict['net_received'] = None
        try:
            output_dict['net_received_units'] = src_dict['Net Received Units']
        except:
            output_dict['net_received_units'] = None
        try:
            output_dict['open_purchase_order_quantity'] = src_dict['Open Purchase Order Quantity']
        except:
            output_dict['open_purchase_order_quantity'] = None
        try:
            output_dict['receive_fill_rate'] = src_dict['Receive Fill Rate %']
        except:
            output_dict['receive_fill_rate'] = None
        try:
            output_dict['overall_vendor_lead_time_days'] = src_dict['Overall Vendor Lead Time (days)']
        except:
            output_dict['overall_vendor_lead_time_days'] = None
        try:
            output_dict['unfilled_customer_ordered_units'] = src_dict['Unfilled Customer Ordered Units']
        except:
            output_dict['unfilled_customer_ordered_units'] = None
        try:
            output_dict['aged_90_days_sellable_inventory'] = src_dict['Aged 90+ Days Sellable Inventory']
        except:
            output_dict['aged_90_days_sellable_inventory'] = None
        try:
            output_dict['aged_90_days_sellable_units'] = src_dict['Aged 90+ Days Sellable Units']
        except:
            output_dict['aged_90_days_sellable_units'] = None
        try:
            output_dict['sellable_on_hand_inventory'] = src_dict['Sellable On-Hand Inventory']
        except:
            output_dict['sellable_on_hand_inventory'] = None
        try:
            output_dict['sellable_on_hand_units'] = src_dict['Sellable On Hand Units']
        except:
            output_dict['sellable_on_hand_units'] = None
        try:
            output_dict['unsellable_on_hand_inventory'] = src_dict['Unsellable On-Hand Inventory']
        except:
            output_dict['unsellable_on_hand_inventory'] = None
        try:
            output_dict['unsellable_on_hand_units'] = src_dict['Unsellable On-Hand Units']
        except:
            output_dict['unsellable_on_hand_units'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None


        return output_dict
    

    @staticmethod
    def amz_emea_de_sales(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['ordered_revenue'] = src_dict['Ordered revenue']
        except:
            output_dict['ordered_revenue'] = None
        try:
            output_dict['ordered_units'] = src_dict['Ordered units']
        except:
            output_dict['ordered_units'] = None
        try:
            output_dict['dispatched_revenue'] = src_dict['Dispatched revenue']
        except:
            output_dict['dispatched_revenue'] = None
        try:
            output_dict['dispatched_cogs'] = src_dict['Dispatched COGS']
        except:
            output_dict['dispatched_cogs'] = None
        try:
            output_dict['dispatched_units'] = src_dict['Dispatched units']
        except:
            output_dict['dispatched_units'] = None
        try:
            output_dict['customer_returns'] = src_dict['Customer returns']
        except:
            output_dict['customer_returns'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    @staticmethod
    def amz_emea_de_traffic(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['glance_views'] = src_dict['Glance views']
        except:
            output_dict['glance_views'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    

    @staticmethod
    def amz_emea_gb_inventory(src_dict:dict):
        output_dict = {}
        # Mapping Attachment Fields
        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['sourceable_product_oos'] = src_dict['Sourceable Product OOS']
        except:
            output_dict['sourceable_product_oos'] = None
        try:
            output_dict['vendor_confirmation_rate'] = src_dict['Vendor Confirmation Rate']
        except:
            output_dict['vendor_confirmation_rate'] = None
        try:
            output_dict['net_received'] = src_dict['Net Received']
        except:
            output_dict['net_received'] = None
        try:
            output_dict['net_received_units'] = src_dict['Net Received Units']
        except:
            output_dict['net_received_units'] = None
        try:
            output_dict['open_purchase_order_quantity'] = src_dict['Open Purchase Order Quantity']
        except:
            output_dict['open_purchase_order_quantity'] = None
        try:
            output_dict['receive_fill_rate'] = src_dict['Receive Fill Rate %']
        except:
            output_dict['receive_fill_rate'] = None
        try:
            output_dict['overall_vendor_lead_time_days'] = src_dict['Overall Vendor Lead Time (days)']
        except:
            output_dict['overall_vendor_lead_time_days'] = None
        try:
            output_dict['unfilled_customer_ordered_units'] = src_dict['Unfilled Customer Ordered Units']
        except:
            output_dict['unfilled_customer_ordered_units'] = None
        try:
            output_dict['aged_90_days_sellable_inventory'] = src_dict['Aged 90+ Days Sellable Inventory']
        except:
            output_dict['aged_90_days_sellable_inventory'] = None
        try:
            output_dict['aged_90_days_sellable_units'] = src_dict['Aged 90+ Days Sellable Units']
        except:
            output_dict['aged_90_days_sellable_units'] = None
        try:
            output_dict['sellable_on_hand_inventory'] = src_dict['Sellable On-Hand Inventory']
        except:
            output_dict['sellable_on_hand_inventory'] = None
        try:
            output_dict['sellable_on_hand_units'] = src_dict['Sellable On Hand Units']
        except:
            output_dict['sellable_on_hand_units'] = None
        try:
            output_dict['unsellable_on_hand_inventory'] = src_dict['Unsellable On-Hand Inventory']
        except:
            output_dict['unsellable_on_hand_inventory'] = None
        try:
            output_dict['unsellable_on_hand_units'] = src_dict['Unsellable On-Hand Units']
        except:
            output_dict['unsellable_on_hand_units'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None


        return output_dict
    
    @staticmethod
    def amz_emea_gb_sales(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['ordered_revenue'] = src_dict['Ordered revenue']
        except:
            output_dict['ordered_revenue'] = None
        try:
            output_dict['ordered_units'] = src_dict['Ordered units']
        except:
            output_dict['ordered_units'] = None
        try:
            output_dict['dispatched_revenue'] = src_dict['Dispatched revenue']
        except:
            output_dict['dispatched_revenue'] = None
        try:
            output_dict['dispatched_cogs'] = src_dict['Dispatched COGS']
        except:
            output_dict['dispatched_cogs'] = None
        try:
            output_dict['dispatched_units'] = src_dict['Dispatched units']
        except:
            output_dict['dispatched_units'] = None
        try:
            output_dict['customer_returns'] = src_dict['Customer returns']
        except:
            output_dict['customer_returns'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    @staticmethod
    def amz_emea_gb_traffic(src_dict:dict):
        output_dict = {}

        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['product_title'] = src_dict['Product title']
        except:
            output_dict['product_title'] = None
        try:
            output_dict['brand'] = src_dict['Brand']
        except:
            output_dict['brand'] = None
        try:
            output_dict['glance_views'] = src_dict['Glance views']
        except:
            output_dict['glance_views'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date'].strftime('%Y-%m-%d')
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date'].strftime('%Y-%m-%d')
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    def shopify_daily_inventory_update(src_dict:dict):
        output_dict = {}
        try:
            output_dict['Handle'] = src_dict['handle']
        except:
            output_dict['Handle'] = None
        try:
            output_dict['Title'] = src_dict['title']
        except:
            output_dict['Title'] = None
        try:
            output_dict['Option1 Name'] = src_dict['option_1_name']
        except:
            output_dict['Option1 Name'] = None
        try:
            output_dict['Option1 Value'] = src_dict['option_1_value']
        except:
            output_dict['Option1 Value'] = None
        try:
            output_dict['Option2 Name'] = src_dict['option_2_name']
        except:
            output_dict['Option2 Name'] = None
        try:
            output_dict['Option2 Value'] = src_dict['option_2_value']
        except:
            output_dict['Option2 Value'] = None
        try:
            output_dict['Option3 Name'] = src_dict['option_3_name']
        except:
            output_dict['Option3 Name'] = None
        try:
            output_dict['Option3 Value'] = src_dict['option_3_value']
        except:
            output_dict['Option3 Value'] = None
        try:
            output_dict['SKU'] = src_dict['sku']
        except:
            output_dict['SKU'] = None
        try:
            output_dict['HS Code'] = src_dict['hs_code']
        except:
            output_dict['HS Code'] = None
        try:
            output_dict['COO'] = src_dict['coo']
        except:
            output_dict['COO'] = None
        try:
            output_dict['Location'] = src_dict['location']
        except:
            output_dict['Location'] = None
        try:
            output_dict['Incoming'] = src_dict['incoming']
        except:
            output_dict['Incoming'] = None
        try:
            output_dict['Unavailable'] = src_dict['unavailable']
        except:
            output_dict['Unavailable'] = None
        try:
            output_dict['Committed'] = src_dict['committed']
        except:
            output_dict['Committed'] = None
        try:
            output_dict['Available'] = src_dict['available']
        except:
            output_dict['Available'] = None
        try:
            output_dict['On hand'] = src_dict['on_hand']
        except:
            output_dict['On hand'] = None

        return output_dict
    
    def cms_apple_inventory(src_dict:dict):
        output_dict = {}

        try:
            output_dict['region'] = src_dict['Region']
        except:
            output_dict['region'] = None
        try:
            output_dict['plant'] = src_dict['Plant']
        except:
            output_dict['plant'] = None
        try:
            output_dict['customer_product_id'] = src_dict['Customer Product ID']
        except:
            output_dict['customer_product_id'] = None
        try:
            output_dict['product_description'] = src_dict['Product Description']
        except:
            output_dict['product_description'] = None
        try:
            output_dict['manufacturer_product_id'] = src_dict['Manufacturer Product ID']
        except:
            output_dict['manufacturer_product_id'] = None
        try:
            output_dict['manufacturer'] = src_dict['Manufacturer']
        except:
            output_dict['manufacturer'] = None
        try:
            output_dict['apple_hub_aoi_qty'] = src_dict['Apple Hub AOI Qty']
        except:
            output_dict['apple_hub_aoi_qty'] = None
        try:
            output_dict['apple_retail_aoi_qty'] = src_dict['Apple Retail AOI Qty']
        except:
            output_dict['apple_retail_aoi_qty'] = None
        try:
            output_dict['apple_hub_soi_qty'] = src_dict['Apple Hub SOI Qty']
        except:
            output_dict['apple_hub_soi_qty'] = None
        try:
            output_dict['apple_retail_soi_qty'] = src_dict['Apple Retail SOI Qty']
        except:
            output_dict['apple_retail_soi_qty'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    

    def cms_inventory(src_dict:dict):
        output_dict = {}

        try:
            output_dict['region'] = src_dict['Region']
        except:
            output_dict['region'] = None
        try:
            output_dict['plant'] = src_dict['Plant']
        except:
            output_dict['plant'] = None
        try:
            output_dict['customer_product_id'] = src_dict['Customer Product ID']
        except:
            output_dict['customer_product_id'] = None
        try:
            output_dict['product_id'] = src_dict['Product ID']
        except:
            output_dict['product_id'] = None
        try:
            output_dict['product_description'] = src_dict['Product Description']
        except:
            output_dict['product_description'] = None
        try:
            output_dict['manufacturer_product_id'] = src_dict['Manufacturer Product ID']
        except:
            output_dict['manufacturer_product_id'] = None
        try:
            output_dict['manufacturer'] = src_dict['Manufacturer']
        except:
            output_dict['manufacturer'] = None
        try:
            output_dict['cms_warehouse_qty_soi'] = src_dict['CMS Warehouse Qty (SOI)']
        except:
            output_dict['cms_warehouse_qty_soi'] = None
        try:
            output_dict['cms_in_transit_to_apple_qty_soi'] = src_dict['CMS In Transit to Apple Qty (SOI)']
        except:
            output_dict['cms_in_transit_to_apple_qty_soi'] = None
        try:
            output_dict['apple_aoi_open_box_returns'] = src_dict['Apple (AOI) open box returns']
        except:
            output_dict['apple_aoi_open_box_returns'] = None
        try:
            output_dict['apple_aoi_closed_box_returns'] = src_dict['Apple (AOI) closed box returns']
        except:
            output_dict['apple_aoi_closed_box_returns'] = None
        try:
            output_dict['eol_excess_good_stock_returns'] = src_dict['EOL/Excess good stock returns']
        except:
            output_dict['eol_excess_good_stock_returns'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    def cms_forecast(src_dict:dict):
        output_dict = {}

        try:
            output_dict['manufacturer'] = src_dict['Manufacturer']
        except:
            output_dict['manufacturer'] = None
        try:
            output_dict['manufacturer_product_id'] = src_dict['Manufacturer Product ID']
        except:
            output_dict['manufacturer_product_id'] = None
        try:
            output_dict['apple_part_no'] = src_dict['Apple Part No.']
        except:
            output_dict['apple_part_no'] = None
        try:
            output_dict['description_part_no'] = src_dict['Description Part No']
        except:
            output_dict['description_part_no'] = None
        try:
            output_dict['cms_part_no'] = src_dict['CMS Part No']
        except:
            output_dict['cms_part_no'] = None
        try:
            output_dict['prod_description'] = src_dict['Prod Description']
        except:
            output_dict['prod_description'] = None
        try:
            output_dict['region'] = src_dict['Region']
        except:
            output_dict['region'] = None
        try:
            output_dict['plant_id'] = src_dict['Plant ID']
        except:
            output_dict['plant_id'] = None
        try:
            output_dict['plant'] = src_dict['Plant']
        except:
            output_dict['plant'] = None
        try:
            output_dict['report_date'] = parse(src_dict['Report_Date']).strftime('%Y-%m-%d')
        except:
            output_dict['report_date'] = None
        try:
            output_dict['cw'] = src_dict['CW']
        except:
            output_dict['cw'] = None
        try:
            output_dict['cw_1'] = src_dict['CW+1']
        except:
            output_dict['cw_1'] = None
        try:
            output_dict['cw_2'] = src_dict['CW+2']
        except:
            output_dict['cw_2'] = None
        try:
            output_dict['cw_3'] = src_dict['CW+3']
        except:
            output_dict['cw_3'] = None
        try:
            output_dict['cw_4'] = src_dict['CW+4']
        except:
            output_dict['cw_4'] = None
        try:
            output_dict['cw_5'] = src_dict['CW+5']
        except:
            output_dict['cw_5'] = None
        try:
            output_dict['cw_6'] = src_dict['CW+6']
        except:
            output_dict['cw_6'] = None
        try:
            output_dict['cw_7'] = src_dict['CW+7']
        except:
            output_dict['cw_7'] = None
        try:
            output_dict['cw_8'] = src_dict['CW+8']
        except:
            output_dict['cw_8'] = None
        try:
            output_dict['cw_9'] = src_dict['CW+9']
        except:
            output_dict['cw_9'] = None
        try:
            output_dict['cw_10'] = src_dict['CW+10']
        except:
            output_dict['cw_10'] = None
        try:
            output_dict['cw_11'] = src_dict['CW+11']
        except:
            output_dict['cw_11'] = None
        try:
            output_dict['cw_12'] = src_dict['CW+12']
        except:
            output_dict['cw_12'] = None
        try:
            output_dict['cw_13'] = src_dict['CW+13']
        except:
            output_dict['cw_13'] = None
        try:
            output_dict['cw_14'] = src_dict['CW+14']
        except:
            output_dict['cw_14'] = None
        try:
            output_dict['cw_15'] = src_dict['CW+15']
        except:
            output_dict['cw_15'] = None
        try:
            output_dict['cw_16'] = src_dict['CW+16']
        except:
            output_dict['cw_16'] = None
        try:
            output_dict['cw_17'] = src_dict['CW+17']
        except:
            output_dict['cw_17'] = None
        try:
            output_dict['cw_18'] = src_dict['CW+18']
        except:
            output_dict['cw_18'] = None
        try:
            output_dict['cw_19'] = src_dict['CW+19']
        except:
            output_dict['cw_19'] = None
        try:
            output_dict['cw_20'] = src_dict['CW+20']
        except:
            output_dict['cw_20'] = None
        try:
            output_dict['cw_21'] = src_dict['CW+21']
        except:
            output_dict['cw_21'] = None
        try:
            output_dict['cw_22'] = src_dict['CW+22']
        except:
            output_dict['cw_22'] = None
        try:
            output_dict['cw_23'] = src_dict['CW+23']
        except:
            output_dict['cw_23'] = None
        try:
            output_dict['cw_24'] = src_dict['CW+24']
        except:
            output_dict['cw_24'] = None
        try:
            output_dict['cw_25'] = src_dict['CW+25']
        except:
            output_dict['cw_25'] = None
        try:
            output_dict['cw_26'] = src_dict['CW+26']
        except:
            output_dict['cw_26'] = None
        try:
            output_dict['total'] = src_dict['Total']
        except:
            output_dict['total'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    def cms_sales(src_dict:dict):
        output_dict = {}

        try:
            output_dict['vendor'] = src_dict['Vendor']
        except:
            output_dict['vendor'] = None
        try:
            output_dict['manufacturer_product_id'] = src_dict['Manufacturer Product ID']
        except:
            output_dict['manufacturer_product_id'] = None
        try:
            output_dict['cms_part_no'] = src_dict['CMS Part No']
        except:
            output_dict['cms_part_no'] = None
        try:
            output_dict['description_part_no'] = src_dict['Description Part No']
        except:
            output_dict['description_part_no'] = None
        try:
            output_dict['region'] = src_dict['Region']
        except:
            output_dict['region'] = None
        try:
            output_dict['apple_part_no'] = src_dict['Apple Part No.']
        except:
            output_dict['apple_part_no'] = None
        try:
            output_dict['plant'] = src_dict['Plant']
        except:
            output_dict['plant'] = None
        try:
            output_dict['report_date'] = parse(src_dict['Report_Date']).strftime('%Y-%m-%d')
        except:
            output_dict['report_date'] = None
        try:
            output_dict['retail_5'] = src_dict['Retail_5']
        except:
            output_dict['retail_5'] = None
        try:
            output_dict['retail_4'] = src_dict['Retail_4']
        except:
            output_dict['retail_4'] = None
        try:
            output_dict['retail_3'] = src_dict['Retail_3']
        except:
            output_dict['retail_3'] = None
        try:
            output_dict['retail_2'] = src_dict['Retail_2']
        except:
            output_dict['retail_2'] = None
        try:
            output_dict['retail_1'] = src_dict['Retail_1']
        except:
            output_dict['retail_1'] = None
        try:
            output_dict['online_5'] = src_dict['Online_5']
        except:
            output_dict['online_5'] = None
        try:
            output_dict['online_4'] = src_dict['Online_4']
        except:
            output_dict['online_4'] = None
        try:
            output_dict['online_3'] = src_dict['Online_3']
        except:
            output_dict['online_3'] = None
        try:
            output_dict['online_2'] = src_dict['Online_2']
        except:
            output_dict['online_2'] = None
        try:
            output_dict['online_1'] = src_dict['Online_1']
        except:
            output_dict['online_1'] = None
        try:
            output_dict['other_5'] = src_dict['Other_5']
        except:
            output_dict['other_5'] = None
        try:
            output_dict['other_4'] = src_dict['Other_4']
        except:
            output_dict['other_4'] = None
        try:
            output_dict['other_3'] = src_dict['Other_3']
        except:
            output_dict['other_3'] = None
        try:
            output_dict['other_2'] = src_dict['Other_2']
        except:
            output_dict['other_2'] = None
        try:
            output_dict['other_1'] = src_dict['Other_1']
        except:
            output_dict['other_1'] = None
        try:
            output_dict['total_5'] = src_dict['Total_5']
        except:
            output_dict['total_5'] = None
        try:
            output_dict['total_4'] = src_dict['Total_4']
        except:
            output_dict['total_4'] = None
        try:
            output_dict['total_3'] = src_dict['Total_3']
        except:
            output_dict['total_3'] = None
        try:
            output_dict['total_2'] = src_dict['Total_2']
        except:
            output_dict['total_2'] = None
        try:
            output_dict['total_1'] = src_dict['Total_1']
        except:
            output_dict['total_1'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    

    def amz_us_po(src_dict:dict):
        output_dict = {}

        try:
            output_dict['po'] = src_dict['PO']
        except:
            output_dict['po'] = None
        try:
            output_dict['vendor_code'] = src_dict['Vendor code']
        except:
            output_dict['vendor_code'] = None
        try:
            output_dict['order_date'] = src_dict['Order date'].strftime('%Y-%m-%d') 
        except:
            output_dict['order_date'] = None
        try:
            output_dict['status'] = src_dict['Status']
        except:
            output_dict['status'] = None
        try:
            output_dict['priority'] = src_dict['Priority']
        except:
            output_dict['priority'] = None
        try:
            output_dict['product_name'] = src_dict['Product name']
        except:
            output_dict['product_name'] = None
        try:
            output_dict['asin'] = src_dict['ASIN']
        except:
            output_dict['asin'] = None
        try:
            output_dict['external_id_type'] = src_dict['External ID type']
        except:
            output_dict['external_id_type'] = None
        try:
            output_dict['external_id'] = src_dict['External ID']
        except:
            output_dict['external_id'] = None
        try:
            output_dict['model_number'] = src_dict['Model number']
        except:
            output_dict['model_number'] = None
        try:
            output_dict['merchant_sku'] = src_dict['Merchant SKU']
        except:
            output_dict['merchant_sku'] = None
        try:
            output_dict['catalog_number'] = src_dict['Catalog number']
        except:
            output_dict['catalog_number'] = None
        try:
            output_dict['availability'] = src_dict['Availability']
        except:
            output_dict['availability'] = None
        try:
            output_dict['requested_quantity'] = src_dict['Requested quantity']
        except:
            output_dict['requested_quantity'] = None
        try:
            output_dict['accepted_quantity'] = src_dict['Accepted quantity']
        except:
            output_dict['accepted_quantity'] = None
        try:
            output_dict['asn_quantity'] = src_dict['ASN quantity']
        except:
            output_dict['asn_quantity'] = None
        try:
            output_dict['received_quantity'] = src_dict['Received quantity']
        except:
            output_dict['received_quantity'] = None
        try:
            output_dict['cancelled_quantity'] = src_dict['Cancelled quantity']
        except:
            output_dict['cancelled_quantity'] = None
        try:
            output_dict['remaining_quantity'] = src_dict['Remaining quantity']
        except:
            output_dict['remaining_quantity'] = None
        try:
            output_dict['ship_to_location'] = src_dict['Ship-to location']
        except:
            output_dict['ship_to_location'] = None
        try:
            output_dict['window_start'] =  src_dict['Window start'].strftime('%Y-%m-%d')
        except:
            output_dict['window_start'] = None
        try:
            output_dict['window_end'] = src_dict['Window end'].strftime('%Y-%m-%d')
        except:
            output_dict['window_end'] = None
        try:
            output_dict['case_size'] = src_dict['Case size']
        except:
            output_dict['case_size'] = None
        try:
            output_dict['cost'] = src_dict['Cost']
        except:
            output_dict['cost'] = None
        try:
            output_dict['currency'] = src_dict['Currency']
        except:
            output_dict['currency'] = None
        try:
            output_dict['total_requested_cost'] = src_dict['Total requested cost']
        except:
            output_dict['total_requested_cost'] = None
        try:
            output_dict['total_accepted_cost'] = src_dict['Total accepted cost']
        except:
            output_dict['total_accepted_cost'] = None
        try:
            output_dict['total_received_cost'] = src_dict['Total received cost']
        except:
            output_dict['total_received_cost'] = None
        try:
            output_dict['total_cancelled_cost'] = src_dict['Total cancelled cost']
        except:
            output_dict['total_cancelled_cost'] = None
        try:
            output_dict['expected_date'] = src_dict['Expected date'].strftime('%Y-%m-%d')
        except:
            output_dict['expected_date'] = None
        try:
            output_dict['freight_terms'] = src_dict['Freight terms']
        except:
            output_dict['freight_terms'] = None
        try:
            output_dict['consolidation_id'] = src_dict['Consolidation ID']
        except:
            output_dict['consolidation_id'] = None
        try:
            output_dict['cancellation_deadline'] = src_dict['Cancellation deadline'].strftime('%Y-%m-%d')
        except:
            output_dict['cancellation_deadline'] = None

        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict
    
    def amz_search_query_performance(src_dict:dict):
        output_dict = {}

        try:
            output_dict['search_query'] = src_dict['Search Query']
        except:
            output_dict['search_query'] = None
        try:
            output_dict['search_query_score'] = src_dict['Search Query Score']
        except:
            output_dict['search_query_score'] = None
        try:
            output_dict['search_query_volume'] = src_dict['Search Query Volume']
        except:
            output_dict['search_query_volume'] = None
        try:
            output_dict['impressions_total_count'] = src_dict['Impressions: Total Count']
        except:
            output_dict['impressions_total_count'] = None
        try:
            output_dict['impressions_brand_count'] = src_dict['Impressions: Brand Count']
        except:
            output_dict['impressions_brand_count'] = None
        try:
            output_dict['impressions_brand_share_percent'] = src_dict['Impressions: Brand Share %']
        except:
            output_dict['impressions_brand_share_percent'] = None
        try:
            output_dict['clicks_total_count'] = src_dict['Clicks: Total Count']
        except:
            output_dict['clicks_total_count'] = None
        try:
            output_dict['clicks_click_rate_percent'] = src_dict['Clicks: Click Rate %']
        except:
            output_dict['clicks_click_rate_percent'] = None
        try:
            output_dict['clicks_brand_count'] = src_dict['Clicks: Brand Count']
        except:
            output_dict['clicks_brand_count'] = None
        try:
            output_dict['clicks_brand_share_percent'] = src_dict['Clicks: Brand Share %']
        except:
            output_dict['clicks_brand_share_percent'] = None
        try:
            output_dict['clicks_price_median'] = src_dict['Clicks: Price (Median)']
        except:
            output_dict['clicks_price_median'] = None
        try:
            output_dict['clicks_brand_price_median'] = src_dict['Clicks: Brand Price (Median)']
        except:
            output_dict['clicks_brand_price_median'] = None
        try:
            output_dict['clicks_same_day_shipping_speed'] = src_dict['Clicks: Same Day Shipping Speed']
        except:
            output_dict['clicks_same_day_shipping_speed'] = None
        try:
            output_dict['clicks_1d_shipping_speed'] = src_dict['Clicks: 1D Shipping Speed']
        except:
            output_dict['clicks_1d_shipping_speed'] = None
        try:
            output_dict['clicks_2d_shipping_speed'] = src_dict['Clicks: 2D Shipping Speed']
        except:
            output_dict['clicks_2d_shipping_speed'] = None
        try:
            output_dict['cart_adds_total_count'] = src_dict['Cart Adds: Total Count']
        except:
            output_dict['cart_adds_total_count'] = None
        try:
            output_dict['cart_adds_cart_add_rate_percent'] = src_dict['Cart Adds: Cart Add Rate %']
        except:
            output_dict['cart_adds_cart_add_rate_percent'] = None
        try:
            output_dict['cart_adds_brand_count'] = src_dict['Cart Adds: Brand Count']
        except:
            output_dict['cart_adds_brand_count'] = None
        try:
            output_dict['cart_adds_brand_share_percent'] = src_dict['Cart Adds: Brand Share %']
        except:
            output_dict['cart_adds_brand_share_percent'] = None
        try:
            output_dict['cart_adds_price_median'] = src_dict['Cart Adds: Price (Median)']
        except:
            output_dict['cart_adds_price_median'] = None
        try:
            output_dict['cart_adds_brand_price_median'] = src_dict['Cart Adds: Brand Price (Median)']
        except:
            output_dict['cart_adds_brand_price_median'] = None
        try:
            output_dict['cart_adds_same_day_shipping_speed'] = src_dict['Cart Adds: Same Day Shipping Speed']
        except:
            output_dict['cart_adds_same_day_shipping_speed'] = None
        try:
            output_dict['cart_adds_1d_shipping_speed'] = src_dict['Cart Adds: 1D Shipping Speed']
        except:
            output_dict['cart_adds_1d_shipping_speed'] = None
        try:
            output_dict['cart_adds_2d_shipping_speed'] = src_dict['Cart Adds: 2D Shipping Speed']
        except:
            output_dict['cart_adds_2d_shipping_speed'] = None
        try:
            output_dict['purchases_total_count'] = src_dict['Purchases: Total Count']
        except:
            output_dict['purchases_total_count'] = None
        try:
            output_dict['purchases_purchase_rate_percent'] = src_dict['Purchases: Purchase Rate %']
        except:
            output_dict['purchases_purchase_rate_percent'] = None
        try:
            output_dict['purchases_brand_count'] = src_dict['Purchases: Brand Count']
        except:
            output_dict['purchases_brand_count'] = None
        try:
            output_dict['purchases_brand_share_percent'] = src_dict['Purchases: Brand Share %']
        except:
            output_dict['purchases_brand_share_percent'] = None
        try:
            output_dict['purchases_price_median'] = src_dict['Purchases: Price (Median)']
        except:
            output_dict['purchases_price_median'] = None
        try:
            output_dict['purchases_brand_price_median'] = src_dict['Purchases: Brand Price (Median)']
        except:
            output_dict['purchases_brand_price_median'] = None
        try:
            output_dict['purchases_same_day_shipping_speed'] = src_dict['Purchases: Same Day Shipping Speed']
        except:
            output_dict['purchases_same_day_shipping_speed'] = None
        try:
            output_dict['purchases_1d_shipping_speed'] = src_dict['Purchases: 1D Shipping Speed']
        except:
            output_dict['purchases_1d_shipping_speed'] = None
        try:
            output_dict['purchases_2d_shipping_speed'] = src_dict['Purchases: 2D Shipping Speed']
        except:
            output_dict['purchases_2d_shipping_speed'] = None
        try:
            output_dict['reporting_date'] = src_dict['Reporting Date']
        except:
            output_dict['reporting_date'] = None
        try:
            output_dict['week_number'] = src_dict['Week Number']
        except:
            output_dict['week_number'] = None
        try:
            output_dict['start_date'] = src_dict['Start Date']
        except:
            output_dict['start_date'] = None
        try:
            output_dict['end_date'] = src_dict['End Date']
        except:
            output_dict['end_date'] = None
        try:
            output_dict['sharepoint_file_id'] = src_dict['sharepoint_file_id']
        except:
            output_dict['sharepoint_file_id'] = None
        try:
            output_dict['sharepoint_file_name'] = src_dict['sharepoint_file_name']
        except:
            output_dict['sharepoint_file_name'] = None
        try:
            output_dict['pipeline_data_sync_utc_at'] = src_dict['pipeline_data_sync_utc_at']
        except:
            output_dict['pipeline_data_sync_utc_at'] = None

        return output_dict