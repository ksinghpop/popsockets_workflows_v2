from sqlalchemy import MetaData, text, Table, Column, Integer, String, Float
import pandas as pd

class SFLoadTableSchema:
    def __init__(self,engine) -> None:
        self.engine=engine

    def load_metadata(self):
        metadata = MetaData()
        metadata.reflect(self.engine)
        print("Metadata Loaded...",flush=True)
        return metadata

    def load_table(self,tablename):
        metadata = self.load_metadata()
        if tablename not in metadata.tables:
            print(f"{tablename} View loaded...",flush=True)
            return Table(tablename, metadata, autoload_with=self.engine)
        else:
            table = metadata.tables[tablename]
            print(f"{tablename} Table loaded...",flush=True)
            return table
    
    def load_all_tables(self):
        metadata = self.load_metadata()
        print(f"All Tables loaded...",flush=True)
        return metadata.tables
    
    def create_table(self,tablename):
        metadata = self.load_metadata()
        table = metadata.tables[tablename]
        table.create(self.engine)
        print(f"{tablename} created in database...",flush=True)



class DDLObject:

    @staticmethod
    def add_column(tablename:str,new_column_name:str,data_type:str):
        stmt = text(f"""ALTER TABLE {tablename} ADD COLUMN {new_column_name} {data_type}""")
        print(f"Add Columns statement is created:\n{stmt}",flush=True)
        return stmt
    
    @staticmethod
    def create_table(table_obj:Table,conn,replace=False):
        if replace is True:
            table_obj.drop(conn,checkfirst=True)
            table_obj.create(conn)
        else:
            table_obj.create(conn,checkfirst=True)
        return None

def create_table_from_dataframe(df:pd.DataFrame, table_name:str, metadata=MetaData()):
    columns = []
    for col_name, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = Integer
        elif pd.api.types.is_float_dtype(dtype):
            col_type = Float
        elif pd.api.types.is_string_dtype(dtype):
            col_type = String
        else:
            raise ValueError(f"Unsupported dtype {dtype} for column {col_name}")
        columns.append(Column(col_name, col_type))

    return Table(table_name, metadata, *columns)

