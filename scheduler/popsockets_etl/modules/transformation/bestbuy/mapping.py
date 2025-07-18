from dateutil.parser import parse


def best_buy_wkly_edi852_mapping(src_dict:dict,manual_job:bool=False) -> dict:
    email_datetime = None
    email_datetime_str = None
    utc_offset_str = None
    output_dict = {}
    if manual_job is False:
        email_datetime = parse(src_dict['date'])
        email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
        utc_offset_str = str(email_datetime.timetz())
    
    
    # Mapping Attachment Fields
    try:
        output_dict['begin_date'] = src_dict['Begin Date']
    except:
        output_dict['begin_date'] = None
    try:
        output_dict['end_date'] = src_dict['End Date']
    except:
        output_dict['end_date'] = None
    try:
        output_dict['vendor'] = src_dict['Vendor #']
    except:
        output_dict['vendor'] = None
    try:
        output_dict['store'] = src_dict['Store #']
    except:
        output_dict['store'] = None
    try:
        output_dict['duns_location'] = src_dict['DUNS # / Location #']
    except:
        output_dict['duns_location'] = None
    try:
        output_dict['store_name_reporting_location'] = src_dict['Store Name/ Reporting Location']
    except:
        output_dict['store_name_reporting_location'] = None
    try:
        output_dict['dept'] = src_dict['Dept #']
    except:
        output_dict['dept'] = None
    try:
        output_dict['region'] = src_dict['Region']
    except:
        output_dict['region'] = None
    try:
        output_dict['flex_field_1'] = src_dict['Flex Field 1']
    except:
        output_dict['flex_field_1'] = None
    try:
        output_dict['flex_field_2'] = src_dict['Flex Field 2']
    except:
        output_dict['flex_field_2'] = None
    try:
        output_dict['flex_field_3'] = src_dict['Flex Field 3']
    except:
        output_dict['flex_field_3'] = None
    try:
        output_dict['buyer_sku'] = src_dict['Buyer SKU']
    except:
        output_dict['buyer_sku'] = None
    try:
        output_dict['vendor_item'] = str(src_dict['Vendor Item #'])
    except:
        output_dict['vendor_item'] = None
    try:
        output_dict['upc_ean'] = src_dict['UPC/EAN']
    except:
        output_dict['upc_ean'] = None
    try:
        output_dict['item_description'] = src_dict['Item Description']
    except:
        output_dict['item_description'] = None
    try:
        output_dict['qty_on_hold'] = src_dict['Qty on Hold']
    except:
        output_dict['qty_on_hold'] = None
    try:
        output_dict['qty_on_hand'] = src_dict['Qty on Hand']
    except:
        output_dict['qty_on_hand'] = None
    try:
        output_dict['beginning_balance_qty'] = src_dict['Beginning Balance Qty']
    except:
        output_dict['beginning_balance_qty'] = None
    try:
        output_dict['additional_demand_qty'] = src_dict['Additional Demand Qty']
    except:
        output_dict['additional_demand_qty'] = None
    try:
        output_dict['quantity_in_bond'] = src_dict['Quantity In-bond']
    except:
        output_dict['quantity_in_bond'] = None
    try:
        output_dict['qty_in_transit'] = src_dict['Qty in Transit']
    except:
        output_dict['qty_in_transit'] = None
    try:
        output_dict['minimum_inventory_qty'] = src_dict['Minimum Inventory Qty']
    except:
        output_dict['minimum_inventory_qty'] = None
    try:
        output_dict['maximum_inventory_qty'] = src_dict['Maximum Inventory Qty']
    except:
        output_dict['maximum_inventory_qty'] = None
    try:
        output_dict['planned_inventory_qty'] = src_dict['Planned Inventory Qty']
    except:
        output_dict['planned_inventory_qty'] = None
    try:
        output_dict['out_of_stock_qty'] = src_dict['Out of Stock Qty']
    except:
        output_dict['out_of_stock_qty'] = None
    try:
        output_dict['qty_on_order'] = src_dict['Qty on Order']
    except:
        output_dict['qty_on_order'] = None
    try:
        output_dict['qty_received'] = src_dict['Qty Received']
    except:
        output_dict['qty_received'] = None
    try:
        output_dict['qty_sold'] = src_dict['Qty Sold']
    except:
        output_dict['qty_sold'] = None
    try:
        output_dict['adjustment_to_inventory_qty'] = src_dict['Adjustment to Inventory Qty']
    except:
        output_dict['adjustment_to_inventory_qty'] = None
    try:
        output_dict['qty_request_override'] = src_dict['Qty Request Override']
    except:
        output_dict['qty_request_override'] = None
    try:
        output_dict['qty_returned_by_consumer'] = src_dict['Qty Returned By Consumer']
    except:
        output_dict['qty_returned_by_consumer'] = None
    try:
        output_dict['qty_withdrawn_from_whse_inventory'] = src_dict['Qty Withdrawn from Whse Inventory']
    except:
        output_dict['qty_withdrawn_from_whse_inventory'] = None
    try:
        output_dict['qty_committed'] = src_dict['Qty Committed']
    except:
        output_dict['qty_committed'] = None
    try:
        output_dict['qty_transferred'] = src_dict['Qty Transferred']
    except:
        output_dict['qty_transferred'] = None
    try:
        output_dict['ending_balance_qty'] = src_dict['Ending Balance Qty']
    except:
        output_dict['ending_balance_qty'] = None
    try:
        output_dict['price'] = src_dict['Price']
    except:
        output_dict['price'] = None
    try:
        output_dict['flex_field_7'] = src_dict['Flex Field 7']
    except:
        output_dict['flex_field_7'] = None
    try:
        output_dict['unit_of_measure'] = src_dict['Unit of Measure']
    except:
        output_dict['unit_of_measure'] = None
    try:
        output_dict['promotion_start_date'] = src_dict['Promotion Start Date']
    except:
        output_dict['promotion_start_date'] = None
    try:
        output_dict['promotion_end_date'] = src_dict['Promotion End Date']
    except:
        output_dict['promotion_end_date'] = None
    try:
        output_dict['effective_date'] = src_dict['Effective Date']
    except:
        output_dict['effective_date'] = None
    try:
        output_dict['other_infos'] = src_dict["Other Info / #'s"]
    except:
        output_dict['other_infos'] = None
    try:
        output_dict['contact_name'] = src_dict['Contact Name']
    except:
        output_dict['contact_name'] = None
    try:
        output_dict['contact_phone'] = src_dict['Contact Phone']
    except:
        output_dict['contact_phone'] = None
    try:
        output_dict['contact_fax'] = src_dict['Contact Fax']
    except:
        output_dict['contact_fax'] = None
    try:
        output_dict['contact_email'] = src_dict['Contact Email']
    except:
        output_dict['contact_email'] = None
    try:
        output_dict['planned_order_qty'] = src_dict['Planned Order Qty']
    except:
        output_dict['planned_order_qty'] = None
    try:
        output_dict['retail_sales_qty'] = src_dict['Retail Sales Qty']
    except:
        output_dict['retail_sales_qty'] = None
    try:
        output_dict['quantity_carrier_forward'] = src_dict['Quantity Carrier Forward']
    except:
        output_dict['quantity_carrier_forward'] = None
    try:
        output_dict['dea_number'] = src_dict['DEA Number']
    except:
        output_dict['dea_number'] = None
    try:
        output_dict['item_activity_type'] = src_dict['Item Activity Type']
    except:
        output_dict['item_activity_type'] = None
    try:
        output_dict['qty_sold_net'] = src_dict['Qty Sold (Net)']
    except:
        output_dict['qty_sold_net'] = None
    try:
        output_dict['invoice_number'] = src_dict['Invoice Number']
    except:
        output_dict['invoice_number'] = None
    
    # Mapping email default fields
    try:
        output_dict['email_recieved_datetime'] = email_datetime_str
    except:
        output_dict['email_recieved_datetime'] = None
    try:
        output_dict['email_recieved_datetime_utc_offset'] = utc_offset_str
    except:
        output_dict['email_recieved_datetime_utc_offset'] = None
    try:
        output_dict['email_thread_id'] = src_dict['threadId']
    except:
        output_dict['email_thread_id'] = None
    try:
        output_dict['email_message_id'] = src_dict['messageId']
    except:
        output_dict['email_message_id'] = None
    try:
        output_dict['email_attachment_id'] = src_dict['attachmentId']
    except:
        output_dict['email_attachment_id'] = None
    try:
        output_dict['email_attachment_filename'] = src_dict['filename']
    except:
        output_dict['email_attachment_filename'] = None
    
    return output_dict