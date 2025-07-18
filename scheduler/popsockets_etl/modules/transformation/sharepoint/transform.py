import pandas as pd
import io

def bytes_to_pandas_df(bytes_data:bytes,file_type:str=None):
    if file_type is not None:
        if file_type == 'csv':
            df = pd.read_csv(io.BytesIO(bytes_data), encoding="utf-8")
            return df
        else:
            df = pd.read_excel(io.BytesIO(bytes_data))
            print(df.head(),flush=True)
            return df
    else:
        df = pd.read_excel(io.BytesIO(bytes_data))
        print(df.head(),flush=True)
        return df

def df_to_csv_bytes(df:pd.DataFrame):
    csv_stream = io.BytesIO()
    df.to_csv(csv_stream, index=False, encoding="utf-8")
    csv_stream.seek(0)
    csv_stream.getvalue()
    return csv_stream.getvalue()