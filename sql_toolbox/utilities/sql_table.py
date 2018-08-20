from typing import Dict
from enum import Enum

from sql_toolbox.utilities.sql_refactor import SqlSession


class SqlDataType(Enum):
    null = "NULL"
    integer = "INTEGER"
    real = "REAL"
    text = "TEXT"
    blob = "BLOB"

    @classmethod
    def data_types(cls):
        return [data_type.value for data_type in cls]

class DataTable:

    def __init__(self, *, table_name: str, categories: Dict[str, str], 
                 primary_key: str=False):

        #TODO: Support primary key, better led keyarg be id INTEGER PRIMARY K
        self._primary_key = primary_key
        self._table_name = table_name
        if self.validate_category_type(categories):
            self._categories = categories

    def validate_category_type(self, categories):
        valid_types = SqlDataType.data_types()
        print(valid_types)
        for d_type in list(categories.values()):
            if d_type not in valid_types:
                raise RuntimeError("Not Valid data_type")

        return True

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def categories(self):
        return self._categories

    @property
    def table_name(self):
        return self._table_name


class DataTableTools:

    def __init__(self, *, sql_session: SqlSession):
        self._sql_session = sql_session

    def _does_table_exist(self, sql_ses, table_name):
        sql_ses.cursor.execute("SELECT count(*) FROM sqlite_master where type='table'AND name=?", (table_name,))

        if sql_ses.cursor.fetchall()[0][0] == 1:
            return True

        return False

    def _valid_keys(self, data_table: DataTable,
                         sql_data: Dict[str, str]):
        if data_table.categories.keys() == sql_data.keys():
            return True

        else:
            raise RuntimeError("Some keys are invalid!") 

    def create_table(self, data_table: DataTable):

            with self._sql_session as sql_ses:
                if self._does_table_exist(sql_ses, data_table.table_name):
                    raise Exception("Shait")

                if not data_table.primary_key:
                    text = f"""CREATE TABLE {data_table.table_name} (id INTEGER PRIMARY KEY""" 
                    for cat, sql_type in data_table.categories.items():
                        text += f", {cat} {sql_type}"

                    text += ")"

                else:
                    text = f"""CREATE TABLE {data_table.table_name} (""" 
                    for cat, sql_type in data_table.categories.items():
                        text += f"{cat} {sql_type},"

                    text += "PRIMARY KEY (data_table.primary_key))"
                print(text) 
                sql_ses.cursor.execute(text)




    def write_to_table(self, data_table: DataTable, sql_data: Dict[str, str]):
        #TODO: Make keyword argument to write to table even if all
        #categoies are not present. Ex beautiful soup missing entity from scan.
        if self._valid_keys(data_table, sql_data):
            with self._sql_session as sql_ses:
                text = f"INSERT INTO {data_table.table_name}"
                features = "(" + ",".join(cat for cat in sql_data)+ ")"
                nr_values = "VALUES(" + ",".join("?" for i in
                                                 range(len(sql_data))) + ")"
                
                sql = text + features + nr_values
            
                values = tuple(sql_data.values())
                print(sql, values)
                sql_ses.cursor.execute(sql, values)
                sql_ses.commit()
               

    def purge_table(self, data_table: DataTable):
        sql = f"DELETE FROM {data_table.table_name}"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()
    
    def delete_table(self, data_table: DataTable):
        sql = f"DROP TABLE {data_table.table_name}"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()
