from sqlalchemy import Table, and_, or_, true, false, cast, text
from sqlalchemy.sql.expression import literal_column
from typing import Dict, List
import pandas as pd
import sqlalchemy
# from popsockets_etl.modules.sqlops.snowflake.engine import Engine

class SQLConditions:
    
    @staticmethod
    def and_nest_conditions(conditions:list,include:bool):
        if include is True:
            return and_(true(),*conditions)
        else:
            return and_(false(),*conditions)
    
    @staticmethod
    def or_nest_conditions(conditions:list,include:bool):
        if include is True:
            return or_(true(),*conditions)
        else:
            return or_(false(),*conditions)

    @staticmethod
    def equals_to(field:str,value):
        return literal_column(field)==value
    
    @staticmethod
    def not_equals_to(field:str,value):
        return literal_column(field)!=value
    
    @staticmethod
    def greater_than(field:str,value):
        return literal_column(field)>value
    
    @staticmethod
    def greater_than_eqauls_to(field:str,value):
        return literal_column(field)>=value
    
    @staticmethod
    def less_than(field:str,value):
        return literal_column(field)<value
    
    @staticmethod
    def less_than_eqauls_to(field:str,value):
        return literal_column(field)<=value

    @staticmethod
    def in_column(column:str,values:list):
        return literal_column(column).in_(values)

class SelectQuery:
    def __init__(self,table:Table) -> None:
        self.table=table
    
    def select(self,limit:int=None):
        if limit is None:
            query = self.table.select()
            return query
        else:    
            query = self.table.select().fetch(limit)
            return query
        
    def select_specific_columns(self,columns:list,desc_sort_column:str=None,limit:int=None):
        columns_list = [self.table.columns[name] for name in columns]
        sorting = None
        if desc_sort_column is not None:
            sorting = sqlalchemy.desc(desc_sort_column)

        if limit is None:
            query = None
            if sorting is not None:
                query = sqlalchemy.select(columns_list).order_by(sorting)
            else:
                query = sqlalchemy.select(columns_list)
            return query
        else:
            query = None
            if sorting is not None:
                query = sqlalchemy.select(columns_list).order_by(sorting).limit(limit)
            else:
                query = sqlalchemy.select(columns_list).order_by(sorting)
            return query

    def select_by_daterange(self,start_date:str,end_date:str,start_date_column:str,end_date_column:str):
        filter = SQLConditions()
        conditions = filter.and_nest_conditions(
                                                [filter.greater_than_eqauls_to(field=start_date_column,value=start_date),
                                                 filter.less_than_eqauls_to(field=end_date_column,value=end_date)],
                                                 True
                                            )
        query = self.table.select().where(conditions)                                            
        return query

    def select_specific_columns_by_daterange(self,columns:list,start_date:str,end_date:str,start_date_column:str,end_date_column:str,limit:int=None,desc_sort_column:str=None):
        columns_list = [self.table.columns[name] for name in columns]
        filter = SQLConditions()
        conditions = filter.and_nest_conditions(
                                                [filter.greater_than_eqauls_to(field=start_date_column,value=start_date),
                                                 filter.less_than_eqauls_to(field=end_date_column,value=end_date)],
                                                 True
                                            )
        sorting = None
        if desc_sort_column is not None:
            sorting = sqlalchemy.desc(desc_sort_column)
        if limit is None:
            query = None
            if sorting is not None:
                query = sqlalchemy.select(columns_list).where(conditions).order_by(sorting)
            else:
                query = sqlalchemy.select(columns_list).where(conditions)
            return query
        else:
            query = None
            if sorting is not None:
                query = sqlalchemy.select(columns_list).where(conditions).order_by(sorting).limit(limit)
            else:
                query = sqlalchemy.select(columns_list).where(conditions).order_by(sorting)
            return query
    
    def select_in_chunk(self,limit:int,offset:int):
        query = self.table.select().limit(limit).offset(offset)
        return query
    
    @classmethod
    def select_by_query_str(cls,query_string:str):
        query = text(query_string)
        return query

class InsertQuery:
    def __init__(self,table:Table) -> None:
        self.table=table

    def insert_all(self,serialized_data:List[Dict],cast_data_type:List[Dict]=None):
        query = None
        if cast_data_type is not None:
            for record in serialized_data:
                for dict_ in cast_data_type:
                    record.update({dict_['name']:cast(record[dict_['name']],dict_['type'])})
            query = self.table.insert().values(serialized_data)
        else:
            query = self.table.insert().values(serialized_data)
        
        return query

    def insert(self,serialized_dict:dict):
        query = self.table.insert().values(serialized_dict)
        return query

class DeleteQuery:
    def __init__(self,table:Table) -> None:
        self.table=table

    def delete_by_id(self,id_column_name:str,id_value):
        query = self.table.delete().where(SQLConditions.equals_to(id_column_name,id_value))
        return query
    
    def delete_by_date(self,date_column_name:str,date_column_value:str):
        filter = SQLConditions()
        query = self.table.delete().where(filter.and_nest_conditions(
                                            [filter.greater_than_eqauls_to(field=date_column_name,value=f"{date_column_value}"),
                                             filter.less_than_eqauls_to(field=date_column_name,value=f"{date_column_value}")],True)
                                            )
        return query
    
    def delete_by_ids_list(self,id_column_name:str,id_value_list:list):
        filter = SQLConditions()
        query = self.table.delete().where(filter.in_column(id_column_name,id_value_list))
        return query
    
class TruncateTableQuery:
    def __init__(self,table:Table) -> None:
        self.table=table

    def truncate(self):
        query = text(f"TRUNCATE TABLE {self.table.name}")
        return query

class DataFrameCrudOps:
    
    @staticmethod
    def insert(df:pd.DataFrame,conn,tablename:str,if_exists:str):
        df.to_sql(tablename,con=conn.get_engine(),if_exists=if_exists,index=False)
        records_count = len(df.axes[0])
        return f"{records_count} rows imported in {tablename} table"
    
    @staticmethod
    def get_data_by_query(sqlalchemy_query,conn):
        df = pd.read_sql(sql=sqlalchemy_query,con=conn.get_engine())
        return df
    
    @staticmethod
    def get_data_by_tablename(conn,tablename:str):
        df = pd.read_sql_table(tablename,conn.get_engine())
        return df

class CustomQuery:
    @staticmethod
    def get_query(query_string:str):
        query = text(query_string)
        return query
