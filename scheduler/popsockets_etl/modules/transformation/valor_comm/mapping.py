from dateutil.parser import parse


def valor_communications_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['invoice_date'] = src_dict['Invoice Date']
    except:
        output_dict['invoice_date'] = None
    try:
        output_dict['invoice_number'] = src_dict['Invoice Number']
    except:
        output_dict['invoice_number'] = None
    try:
        output_dict['company_name'] = src_dict['Company Name']
    except:
        output_dict['company_name'] = None
    try:
        output_dict['company_shipping_address'] = src_dict['Company Shipping Address']
    except:
        output_dict['company_shipping_address'] = None
    try:
        output_dict['sku'] = src_dict['SKU']
    except:
        output_dict['sku'] = None
    try:
        output_dict['upc'] = src_dict['UPC']
    except:
        output_dict['upc'] = None
    try:
        output_dict['title'] = src_dict['Title']
    except:
        output_dict['title'] = None
    try:
        output_dict['quantity'] = src_dict['Quantity']
    except:
        output_dict['quantity'] = None
    try:
        output_dict['reseller_type'] = src_dict['Reseller Type']
    except:
        output_dict['reseller_type'] = None
    try:
        output_dict['sales_rep'] = src_dict['Sales Rep']
    except:
        output_dict['sales_rep'] = None
        
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