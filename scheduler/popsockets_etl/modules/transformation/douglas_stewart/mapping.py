from dateutil.parser import parse


def douglas_stewart_wkly_pos_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['company'] = src_dict['Company']
    except:
        output_dict['company'] = None
    try:
        output_dict['customer_name'] = src_dict['Customer Name']
    except:
        output_dict['customer_name'] = None
    try:
        output_dict['customer'] = src_dict['Customer #']
    except:
        output_dict['customer'] = None
    try:
        output_dict['address'] = src_dict['Address']
    except:
        output_dict['address'] = None
    try:
        output_dict['city'] = src_dict['City']
    except:
        output_dict['city'] = None
    try:
        output_dict['state'] = src_dict['State']
    except:
        output_dict['state'] = None
    try:
        output_dict['postal_code'] = src_dict['Postal Code']
    except:
        output_dict['postal_code'] = None
    try:
        output_dict['country_code'] = src_dict['Country Code']
    except:
        output_dict['country_code'] = None
    try:
        output_dict['country_name'] = src_dict['Country Name']
    except:
        output_dict['country_name'] = None
    try:
        output_dict['phone'] = src_dict['Phone']
    except:
        output_dict['phone'] = None
    try:
        output_dict['order_'] = src_dict['Order #']
    except:
        output_dict['order_'] = None
    try:
        output_dict['invoice_date'] = src_dict['Invoice Date']
    except:
        output_dict['invoice_date'] = None
    try:
        output_dict['product'] = src_dict['Product']
    except:
        output_dict['product'] = None
    try:
        output_dict['description'] = src_dict['Description']
    except:
        output_dict['description'] = None
    try:
        output_dict['vendor_sku'] = src_dict['Vendor Sku']
    except:
        output_dict['vendor_sku'] = None
    try:
        output_dict['upc_ean'] = src_dict['UPC/EAN']
    except:
        output_dict['upc_ean'] = None
    try:
        output_dict['category'] = src_dict['Category']
    except:
        output_dict['category'] = None
    try:
        output_dict['serial'] = src_dict['Serial #']
    except:
        output_dict['serial'] = None
    try:
        output_dict['qty'] = src_dict['Qty']
    except:
        output_dict['qty'] = None
    try:
        output_dict['cost_in_united_states_dollars'] = src_dict['Cost in United States Dollars']
    except:
        output_dict['cost_in_united_states_dollars'] = None
    try:
        output_dict['extended_cost_in_united_states_dollars'] = src_dict['Extended Cost in United States Dollars']
    except:
        output_dict['extended_cost_in_united_states_dollars'] = None
    
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


def douglas_stewart_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['company'] = src_dict['Company']
    except:
        output_dict['company'] = None
    try:
        output_dict['vendor_product'] = src_dict['Vendor Product']
    except:
        output_dict['vendor_product'] = None
    try:
        output_dict['sku'] = src_dict['Sku']
    except:
        output_dict['sku'] = None
    try:
        output_dict['description'] = src_dict['Description']
    except:
        output_dict['description'] = None
    try:
        output_dict['category'] = src_dict['Category']
    except:
        output_dict['category'] = None
    try:
        output_dict['on_hand'] = src_dict['On Hand']
    except:
        output_dict['on_hand'] = None
    try:
        output_dict['unavailable'] = src_dict['Unavailable']
    except:
        output_dict['unavailable'] = None
    try:
        output_dict['reserved'] = src_dict['Reserved']
    except:
        output_dict['reserved'] = None
    try:
        output_dict['on_order'] = src_dict['On Order']
    except:
        output_dict['on_order'] = None
    try:
        output_dict['back_order'] = src_dict['Back-Order']
    except:
        output_dict['back_order'] = None
    try:
        output_dict['cost_in_united_states_dollars'] = src_dict['Cost in United States Dollars']
    except:
        output_dict['cost_in_united_states_dollars'] = None
    try:
        output_dict['mtd_qty_sold'] = src_dict['MTD Qty Sold']
    except:
        output_dict['mtd_qty_sold'] = None
    
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