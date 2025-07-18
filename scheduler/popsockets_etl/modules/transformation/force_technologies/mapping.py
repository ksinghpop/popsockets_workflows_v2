from dateutil.parser import parse


def force_technologies_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['accountno'] = src_dict['AccountNo']
    except:
        output_dict['accountno'] = None
    try:
        output_dict['parent_name'] = src_dict['Parent Name']
    except:
        output_dict['parent_name'] = None
    try:
        output_dict['partid'] = src_dict['PartID']
    except:
        output_dict['partid'] = None
    try:
        output_dict['description'] = src_dict['Description']
    except:
        output_dict['description'] = None
    try:
        output_dict['sum_of_quantitysold'] = src_dict['Sum of QuantitySold']
    except:
        output_dict['sum_of_quantitysold'] = None
    try:
        output_dict['sum_of_soh'] = src_dict['Sum of SOH']
    except:
        output_dict['sum_of_soh'] = None
    try:
        output_dict['brand'] = src_dict['Brand']
    except:
        output_dict['brand'] = None
    try:
        output_dict['weekending'] = src_dict['WeekEnding']
    except:
        output_dict['weekending'] = None
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