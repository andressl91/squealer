import pytest 
import time
import tempfile

from pathlib import Path

from squealer.sql_database import DataBase
from squealer.sqlite_session import SqliteSession

n_rows = 30000

def get_lots_of_data(n_rows):

    keys = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    cat = ["TEXT" for i in range(len(keys))]
    categories = dict(zip(keys, cat))

    sql_data = []
    for i in range(n_rows):
        data = [j for j in range(len(keys))]
        sql_data.append(dict(zip(keys, data)))

    return categories, sql_data



def single_write_of_rows(n_rows, memory: bool=False):
    
    td = tempfile.mkdtemp()
    tf = Path(td) / "test_rows.db"
    tf = str(tf)
    db_tools = DataBase(db_path=tf)
    if memory:
        db_tools.set_memory_session

    else:
        db_tools.set_local_session

    categories, sql_data = get_lots_of_data(n_rows=n_rows)

    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]

    for data in sql_data:
        data_table.write(data)


def multiple_write_of_rows(n_rows, memory: bool=False):
    td = tempfile.mkdtemp()
    tf = Path(td) / "test_mrows.db"
    tf = str(tf)
    db_tools = DataBase(db_path=tf)

    if memory:
        db_tools.set_memory_session

    else:
        db_tools.set_local_session


    categories, sql_data = get_lots_of_data(n_rows=n_rows)
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    data_table.multi_write(sql_data)

@pytest.mark.write
def test_benchmark_single_row_write(benchmark):
    in_kwargs = {"n_rows": n_rows, "memory": False} 


    result = benchmark.pedantic(single_write_of_rows, kwargs=in_kwargs) 


@pytest.mark.write
def test_benchmark_single_row_write_memory(benchmark):
    in_kwargs = {"n_rows": n_rows, "memory": True} 

    result = benchmark.pedantic(single_write_of_rows, kwargs=in_kwargs) 


@pytest.mark.write
def test_benchmark_multiple_row_write(benchmark):
    in_kwargs = {"n_rows": n_rows, "memory": False} 

    result = benchmark.pedantic(multiple_write_of_rows, kwargs=in_kwargs) 



@pytest.mark.write
def test_benchmark_multiple_row_write_memory(benchmark):
    in_kwargs = {"n_rows": n_rows, "memory": True} 

    result = benchmark.pedantic(multiple_write_of_rows, kwargs=in_kwargs) 


if __name__ == "__main__":
    pass
