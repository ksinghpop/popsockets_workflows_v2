from dateutil.parser import parse


def books_a_million_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['pub_group'] = src_dict['Pub_Group']
    except:
        output_dict['pub_group'] = None
    try:
        output_dict['isbn'] = src_dict['ISBN']
    except:
        output_dict['isbn'] = None
    try:
        output_dict['sku_number'] = src_dict['Sku_Number']
    except:
        output_dict['sku_number'] = None
    try:
        output_dict['title'] = src_dict['Title']
    except:
        output_dict['title'] = None
    try:
        output_dict['author'] = src_dict['Author']
    except:
        output_dict['author'] = None
    try:
        output_dict['sku_type'] = src_dict['Sku_Type']
    except:
        output_dict['sku_type'] = None
    try:
        output_dict['idate'] = src_dict['IDate']
    except:
        output_dict['idate'] = None
    try:
        output_dict['retail'] = src_dict['Retail']
    except:
        output_dict['retail'] = None
    try:
        output_dict['week1units'] = src_dict['Week1Units']
    except:
        output_dict['week1units'] = None
    try:
        output_dict['week2units'] = src_dict['Week2Units']
    except:
        output_dict['week2units'] = None
    try:
        output_dict['ytd_units'] = src_dict['YTD_Units']
    except:
        output_dict['ytd_units'] = None
    try:
        output_dict['bam_onhand'] = src_dict['BAM_OnHand']
    except:
        output_dict['bam_onhand'] = None
    try:
        output_dict['warehouse_onhand'] = src_dict['Warehouse_OnHand']
    except:
        output_dict['warehouse_onhand'] = None
    try:
        output_dict['qty_onorder'] = src_dict['Qty_OnOrder']
    except:
        output_dict['qty_onorder'] = None
        
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