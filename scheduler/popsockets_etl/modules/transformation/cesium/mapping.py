from dateutil.parser import parse


def cesium_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['date'] = src_dict['Date']
    except:
        output_dict['date'] = None
    try:
        output_dict['reseller_name'] = src_dict['Reseller Name']
    except:
        output_dict['reseller_name'] = None
    try:
        output_dict['address'] = src_dict['Address']
    except:
        output_dict['address'] = None
    try:
        output_dict['sales_rep'] = src_dict['Sales Rep']
    except:
        output_dict['sales_rep'] = None
    try:
        output_dict['name'] = src_dict['Name']
    except:
        output_dict['name'] = None
    try:
        output_dict['display_name'] = src_dict['Display Name']
    except:
        output_dict['display_name'] = None
    try:
        output_dict['mpn'] = src_dict['MPN']
    except:
        output_dict['mpn'] = None
    try:
        output_dict['upc_code'] = src_dict['UPC Code']
    except:
        output_dict['upc_code'] = None
    try:
        output_dict['total_qty_sold'] = src_dict['Total Qty Sold']
    except:
        output_dict['total_qty_sold'] = None
    
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

def cesium_wkly_inventory_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}

    # Mapping Attachment Fields
    try:
        output_dict['sku'] = src_dict['SKU']
    except:
        output_dict['sku'] = None
    try:
        output_dict['brand'] = src_dict['Brand']
    except:
        output_dict['brand'] = None
    try:
        output_dict['description'] = src_dict['Description']
    except:
        output_dict['description'] = None
    try:
        output_dict['product_category'] = src_dict['Product Category']
    except:
        output_dict['product_category'] = None
    try:
        output_dict['model'] = src_dict['Model']
    except:
        output_dict['model'] = None
    try:
        output_dict['quantity_available'] = src_dict['Quantity Available']
    except:
        output_dict['quantity_available'] = None
    
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