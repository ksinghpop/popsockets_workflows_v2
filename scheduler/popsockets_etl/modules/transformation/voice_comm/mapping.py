from dateutil.parser import parse


def voicecomm_wkly_distribution_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['customer'] = src_dict['Customer #']
    except:
        output_dict['customer'] = None
    try:
        output_dict['company_name'] = src_dict['Company Name']
    except:
        output_dict['company_name'] = None
    try:
        output_dict['dba'] = src_dict['DBA']
    except:
        output_dict['dba'] = None
    try:
        output_dict['sku'] = str(src_dict['SKU']).replace("POP-","")
    except:
        output_dict['sku'] = None
    try:
        output_dict['sum_of_qty_sold'] = src_dict['Sum of Qty Sold']
    except:
        output_dict['sum_of_qty_sold'] = None
    try:
        output_dict['reseller_type'] = src_dict['Reseller Type']
    except:
        output_dict['reseller_type'] = None
    try:
        output_dict['rep'] = src_dict['Rep']
    except:
        output_dict['rep'] = None
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

def voicecomm_wkly_sales_mapping(src_dict:dict):
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    
    # Mapping Attachment Fields
    try:
        output_dict['vendor'] = src_dict['Vendor']
    except:
        output_dict['vendor'] = None
    try:
        output_dict['category'] = src_dict['Category']
    except:
        output_dict['category'] = None
    try:
        output_dict['part'] = src_dict['Part']
    except:
        output_dict['part'] = None
    try:
        output_dict['description'] = src_dict['Desc']
    except:
        output_dict['description'] = None
    try:
        output_dict['qty_sold_8_weeks_ago'] = src_dict['Qty sold 8 weeks ago']
    except:
        output_dict['qty_sold_8_weeks_ago'] = None
    try:
        output_dict['qty_sold_7_weeks_ago'] = src_dict['Qty sold 7 weeks ago']
    except:
        output_dict['qty_sold_7_weeks_ago'] = None
    try:
        output_dict['qty_sold_6_weeks_ago'] = src_dict['Qty sold 6 weeks ago']
    except:
        output_dict['qty_sold_6_weeks_ago'] = None
    try:
        output_dict['qty_sold_5_weeks_ago'] = src_dict['Qty sold 5 weeks ago']
    except:
        output_dict['qty_sold_5_weeks_ago'] = None
    try:
        output_dict['qty_sold_4_weeks_ago'] = src_dict['Qty sold 4 weeks ago']
    except:
        output_dict['qty_sold_4_weeks_ago'] = None
    try:
        output_dict['qty_sold_3_weeks_ago'] = src_dict['Qty sold 3 weeks ago']
    except:
        output_dict['qty_sold_3_weeks_ago'] = None
    try:
        output_dict['qty_sold_last_week'] = src_dict['Qty sold last week']
    except:
        output_dict['qty_sold_last_week'] = None
    try:
        output_dict['qty_sold_this_week'] = src_dict['Qty sold this week']
    except:
        output_dict['qty_sold_this_week'] = None
    try:
        output_dict['total_sold'] = src_dict['Total Sold']
    except:
        output_dict['total_sold'] = None
    try:
        output_dict['on_hand'] = src_dict['On Hand']
    except:
        output_dict['on_hand'] = None
    try:
        output_dict['committed'] = src_dict['Committed']
    except:
        output_dict['committed'] = None
    try:
        output_dict['available'] = src_dict['Available']
    except:
        output_dict['available'] = None
    try:
        output_dict['on_po'] = src_dict['On PO']
    except:
        output_dict['on_po'] = None
    try:
        output_dict['status'] = src_dict['Status']
    except:
        output_dict['status'] = None

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