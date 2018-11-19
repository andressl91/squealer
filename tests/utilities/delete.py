
import os
from squealer.sql_table_tools import DataTable, DataTableTools     
from squealer.sqlite_session import SqliteSession
db_path = os.path.join(os.path.dirname(__file__), "..", "data", "test.db")

table_name = "data2"
categories = {"money": "REAL", "time": "REAL"}
data_table = DataTable(table_name=table_name, categories=categories)

sql_session = SqliteSession(db_path=db_path)

db_tools = DataTableTools(sql_session=sql_session)
db_tools.delete_table(data_table)
