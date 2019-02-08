import pytest 
import time
import tempfile

from pathlib import Path

from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession

n_rows = 1000


def get_lots_of_data(n_rows):

    keys = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    cat = ["TEXT" for i in range(len(keys))]
    categories = dict(zip(keys, cat))

    sql_data = []
    for i in range(n_rows):
        data = [j for j in range(len(keys))]
        sql_data.append(dict(zip(keys, data)))

    return categories, sql_data

def write_read_memory(n_rows, memory):
    td = tempfile.mkdtemp()
    tf = Path(td) / "test_mrows.db"
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)

    db_tools.set_memory_session

    categories, sql_data = get_lots_of_data(n_rows=n_rows)
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    data_table.multi_write(sql_data)
    

    data_table.select("*")


def write_read_local(n_rows, memory):
    td = tempfile.mkdtemp()
    tf = Path(td) / "test_mrows.db"
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)

    if memory:
        db_tools.set_memory_session

    else:
        db_tools.set_local_session

    categories, sql_data = get_lots_of_data(n_rows=n_rows)
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    data_table.multi_write(sql_data)
    

    data_table.select("*")

def load_db_to_memory(n_rows, memory):
    td = tempfile.mkdtemp()
    tf = Path(td) / f"test_{memory}_rows.db"
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)

    categories, sql_data = get_lots_of_data(n_rows=n_rows)
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    data_table.multi_write(sql_data)
    
    if memory == "stringio":
        db_tools.load_to_memory_stringio()
    else:
        db_tools.load_to_memory()

    db_tools.memory_db.close_db()

def write_memory_persist_local(n_rows, memory):
    td = tempfile.mkdtemp()
    tf = Path(td) / "test_mrows.db"
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)

    if memory:
        db_tools.set_memory_session

        categories, sql_data = get_lots_of_data(n_rows=n_rows)
        db_tools.create_table(table_name="data",
                              categories=categories)

        data_table = db_tools.tables["data"]
        data_table.multi_write(sql_data)
        db_tools.load_memory_to_local()

    else:
        db_tools.set_local_session
        categories, sql_data = get_lots_of_data(n_rows=n_rows)
        db_tools.create_table(table_name="data",
                              categories=categories)

        data_table = db_tools.tables["data"]
        data_table.multi_write(sql_data)


@pytest.mark.load_memory
def test_benchmark_load_data_to_memory_stringio(benchmark):
    result = benchmark(load_db_to_memory, n_rows=n_rows, memory="iostring")


@pytest.mark.load_memory
def test_benchmark_load_data_to_memory(benchmark):
    result = benchmark(load_db_to_memory, n_rows=n_rows, memory="")


@pytest.mark.write_read
def test_benchmark_write_read_memory(benchmark):
    result = benchmark(write_read_memory, n_rows=n_rows, memory=True)

@pytest.mark.write_read
def test_benchmark_write_read_local(benchmark):
    result = benchmark(write_read_memory, n_rows=n_rows, memory=False)

@pytest.mark.write_to_local
def test_benchmark_write_memory_persist_local(benchmark):
    result = benchmark(write_read_memory, n_rows=n_rows, memory=True)
#@pytest.mark.memory
@pytest.mark.write_to_local
def test_benchmark_write_local_persist_local(benchmark):
    result = benchmark(write_read_memory, n_rows=n_rows, memory=False)
