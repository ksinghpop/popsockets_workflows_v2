from dateutil.parser import parse


def barnes_and_noble_on_hand_sales_mapping(src_dict:dict):
    last_week_end_date = parse(src_dict['Last Week\nEnd Date'])
    last_week_end_date_str = last_week_end_date.strftime('%Y-%m-%d')
    email_datetime = parse(src_dict['date'])
    email_datetime_str = email_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
    utc_offset_str = str(email_datetime.timetz())
    output_dict = {}
    try:
        output_dict['publisher_name'] = src_dict['Publisher Name']
    except:
        output_dict['publisher_name'] = None
    try:
        output_dict['ean'] = src_dict['EAN']
    except:
        output_dict['ean'] = None
    try:
        output_dict['title'] = src_dict['Title']
    except:
        output_dict['title'] = None
    try:
        output_dict['last_week_end_date'] = src_dict['Last Week\nEnd Date']
    except:
        output_dict['last_week_end_date'] = None
    try:
        output_dict['store_sales_lw_units'] = src_dict['Store Sales\nLW units']
    except:
        output_dict['store_sales_lw_units'] = None
    try:
        output_dict['store_sales_lw_retail'] = src_dict['Store Sales\nLW $Retail']
    except:
        output_dict['store_sales_lw_retail'] = None
    try:
        output_dict['store_sales_4_wk_units'] = src_dict['Store Sales\n4 Wk units']
    except:
        output_dict['store_sales_4_wk_units'] = None
    try:
        output_dict['store_sales_4_wk_retail'] = src_dict['Store Sales\n4 Wk $Retail']
    except:
        output_dict['store_sales_4_wk_retail'] = None
    try:
        output_dict['store_sales_ytd_units'] = src_dict['Store Sales\nYTD units']
    except:
        output_dict['store_sales_ytd_units'] = None
    try:
        output_dict['store_sales_ytd_retail'] = src_dict['Store Sales\nYTD $Retail']
    except:
        output_dict['store_sales_ytd_retail'] = None
    try:
        output_dict['inet_sales_lw_units'] = src_dict['iNet Sales\nLW units']
    except:
        output_dict['inet_sales_lw_units'] = None
    try:
        output_dict['inet_sales_lw_retail'] = src_dict['iNet Sales\nLW $Retail']
    except:
        output_dict['inet_sales_lw_retail'] = None
    try:
        output_dict['inet_sales_4_wk_units'] = src_dict['iNet Sales\n4 Wk units']
    except:
        output_dict['inet_sales_4_wk_units'] = None
    try:
        output_dict['inet_sales_4_wk_retail'] = src_dict['iNet Sales\n4 Wk $Retail']
    except:
        output_dict['inet_sales_4_wk_retail'] = None
    try:
        output_dict['inet_sales_ytd_units'] = src_dict['iNet Sales\nYTD units']
    except:
        output_dict['inet_sales_ytd_units'] = None
    try:
        output_dict['inet_sales_ytd_retail'] = src_dict['iNet Sales\nYTD $Retail    ']
    except:
        output_dict['inet_sales_ytd_retail'] = None
    try:
        output_dict['store_oh_qty'] = src_dict['Store OH\nQty']
    except:
        output_dict['store_oh_qty'] = None
    try:
        output_dict['dc_oh_qty'] = src_dict['DC OH\nQty']
    except:
        output_dict['dc_oh_qty'] = None
    try:
        output_dict['total_qoh'] = src_dict['Total\nQOH']
    except:
        output_dict['total_qoh'] = None
    try:
        output_dict['dc_oo_qty'] = src_dict['DC OO\nQty']
    except:
        output_dict['dc_oo_qty'] = None
    try:
        output_dict['list_price'] = src_dict['List\nPrice']
    except:
        output_dict['list_price'] = None
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