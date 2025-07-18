import pandas as pd
# import win32com.client as win32
# import jpype     
# import asposecells.cells import Workbook
import numpy as np
import magic
import xml.etree.cElementTree as ET
import json
import re


def detect_txt_separator(file_path):
    with open(file_path, 'r',encoding='utf-8', errors='ignore') as file:
        # Read the first line to detect the separator
        line = file.readline()
        
        # Common delimiters
        delimiters = [',', '\t', ';', ' ']
        delimiter_counts = {delimiter: line.count(delimiter) for delimiter in delimiters}
        
        # Find the delimiter with the most occurrences
        detected_delimiter = max(delimiter_counts, key=delimiter_counts.get)
        
        return detected_delimiter

# def xls_to_xlsx(xls_file_path:str,xlsx_file_path:str):
#     excel = win32.gencache.EnsureDispatch('Excel.Application')
#     wb = excel.Workbooks.Open(xls_file_path)
#     excel.DisplayAlerts = False
#     wb.SaveAs(xlsx_file_path,FileFormat = 51)
#     wb.Close(False)
#     excel.Application.Quit()
#     return None

def text_file_encode_to_utf8(src_txt_file_path:str,out_txt_file_path:str):
    with open(src_txt_file_path) as f:
        data_str = f.read()
    
    with open(out_txt_file_path,'w') as f:
        f.write(data_str)



def df_remove_totals_row(src_df_path:str,ext:str,remove_tail:bool,txt_delimeter:str=None):
    engine_list = ('calamine','xlrd','pandas','openpyxl')
    out_df = None
    global_engine = None
    if ext == '.xls' or ext == '.xlsx':
        for engine in engine_list:
            if global_engine is None:
                try:
                    temp_df = pd.read_excel(src_df_path,engine=engine)
                    out_df = temp_df
                    global_engine = engine
                    print(f"{engine} is suitable",flush=True)
                except Exception as e:
                    print(f"{engine} is not suitable due to following exception:\n{e}",flush=True)
                    pass
    elif ext == '.csv':
        temp_df = pd.read_csv(src_df_path)
        out_df = temp_df
    
    elif ext == '.xml':
        temp_df = pd.read_xml(src_df_path)
        out_df = temp_df
    
    elif ext == '.txt':
        # if txt_delimeter is not None:
        #     temp_df = pd.read_csv(src_df_path,delimiter='\t',encoding='utf-8',encoding_errors='replace')
        #     out_df = temp_df
        # else:
        delimeter = detect_txt_separator(src_df_path)
        temp_df = pd.read_csv(src_df_path,delimiter=delimeter,encoding='utf-8',encoding_errors='replace')
        out_df = temp_df
    
    if remove_tail is True:
        out_df.drop(out_df.tail(1).index,inplace = True)
    
    out_df.replace(np.nan, None,inplace=True)
    return out_df

# def xls_to_xlsx2(xls_file_path:str,xlsx_file_path:str):
#     jpype.startJVM() 
#     from asposecells.api import Workbook
#     workbook = Workbook(xls_file_path)
#     workbook.save(xlsx_file_path)
#     jpype.shutdownJVM()
#     return None

# def xml_to_xlsx(xml_file_path:str,xlsx_file_path:str):
#     workbook = Workbook(xml_file_path)
#     workbook.save(xlsx_file_path)
#     return True


def get_mime_type_from_file_path(file_path):
    # Create a Magic object
    mime = magic.Magic(mime=True)
    
    # Determine the MIME type of the file
    mime_type = mime.from_file(file_path)
    return mime_type

def get_mime_type_from_bytes(byte_data):
    # Create a Magic object with mime=True to get the MIME type
    mime = magic.Magic(mime=True)
    
    # Determine the MIME type from the byte string
    mime_type = mime.from_buffer(byte_data)
    return mime_type

def validate_mime_type(file_path=None,bytes_data=None,get_mime_type=False,get_ext=False):
    mime_data_type = None
    if bytes_data is not None:
        mime_data_type = get_mime_type_from_bytes(bytes_data)
    elif file_path is not None:
        mime_data_type = get_mime_type_from_file_path(file_path)
    else:
        raise Exception("Provide value for one of these params:\n-file_path\n-bytes_data")

    if get_mime_type is True:
        return mime_data_type

    if get_ext is True:
        extension = None
        if mime_data_type == 'application/vnd.ms-excel':
            extension = '.xls'
        elif mime_data_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            extension = '.xlsx'
        elif mime_data_type == 'text/csv':
            extension = '.csv'
        elif mime_data_type == 'application/xml' or mime_data_type == 'text/xml':
            extension = '.xml'
        elif mime_data_type == 'text/plain':
            extension = '.txt'
        return extension
    
def microsoft_xml_record_list(src_xml_path:str,namespace:dict,cols:list[str]):
    tree = ET.parse(src_xml_path)
    root = tree.getroot()

    def getvalueofnode(node):
        """ return node text or None """
        return node.text if node is not None else None
    
    data = []

    for i, node in enumerate(root.findall('.//doc:Row', namespace)):
        if i > 6:
            temp_dict = {}
            for pos,col_data in enumerate(cols):
                temp_dict.update({col_data:getvalueofnode(node.find(f'doc:Cell[{pos+1}]/doc:Data', namespace))})

            data.append(temp_dict)

        
    return data

def chunk_list(data, chunk_size):
    """
    Splits a list into smaller chunks of a specified size.
    
    :param data: List to be chunked
    :param chunk_size: Size of each chunk
    :return: A generator with chunks of the list
    """
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def remove_unicode(obj):
    if isinstance(obj, dict):
        return {k: remove_unicode(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_unicode(elem) for elem in obj]
    elif isinstance(obj, str):
        # Remove all unicode characters (outside ASCII)
        return re.sub(r'[^\x00-\x7F]+', '', obj)
    else:
        return obj