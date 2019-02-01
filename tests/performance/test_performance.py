import pytest 
import time
import tempfile
from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession

def get_lots_of_data(keys, n_rows):

    sql_data = []
    for i in range(n_rows):
        data = [j for j in range(len(keys))]
        sql_data.append(dict(zip(keys, data)))

    return sql_data


#Old scool
def test_read_and_write_table():

    with tempfile.TemporaryFile(suffix=".db", prefix="test") as tf:
        tf = str(tf)
        db_tools = DataTableTools(db_path=tf)
        keys = ["a", "b", "c", "d", "e"]
        cat = ["INTEGER" for i in range(len(keys))]
        categories = dict(zip(keys, cat))
        db_tools.create_table(table_name="data",
                              categories=categories)

        # Test write, and order of write features is arbritary
        data_table = db_tools.tables["data"]
        n_rows = 1000
        sql_data = get_lots_of_data(keys, n_rows=n_rows)

        t_serial_write = time.time()
        for data in sql_data:
            data_table.write(data)
        t_serial_write_end = time.time() - t_serial_write

        t_multi_write = time.time()
        data_table.multi_write(sql_data)
        t_multi_write_end = time.time() - t_multi_write


        factor = t_multi_write_end/t_serial_write_end
        print(f"Using {len(categories)} categories with {n_rows} rows.")
        print(f"Single write uses {t_serial_write_end}",
              f"Multi write uses {t_multi_write_end}")
        print(f"Multi write is faster/slower with a factor of {factor}")



def single_write_of_rows(n_rows):
    with tempfile.NamedTemporaryFile(suffix=".db", prefix="test") as tf:
        tf = str(tf)
        db_tools = DataTableTools(db_path=tf)
        keys = ["a", "b", "c", "d", "e"]
        cat = ["INTEGER" for i in range(len(keys))]
        categories = dict(zip(keys, cat))
        db_tools.create_table(table_name="data",
                              categories=categories)

        data_table = db_tools.tables["data"]
        sql_data = get_lots_of_data(keys, n_rows=n_rows)

        for data in sql_data:
            data_table.write(data)

        return len(sql_data)



def multiple_write_of_rows(n_rows):
    with tempfile.NamedTemporaryFile(suffix=".db", prefix="test") as tf:
        tf = str(tf)
        db_tools = DataTableTools(db_path=tf)
        keys = ["a", "b", "c", "d", "e"]
        cat = ["INTEGER" for i in range(len(keys))]
        categories = dict(zip(keys, cat))
        db_tools.create_table(table_name="data",
                              categories=categories)

        data_table = db_tools.tables["data"]
        sql_data = get_lots_of_data(keys, n_rows=n_rows)

        data_table.multi_write(sql_data)

        return len(sql_data)

@pytest.mark.skip
def test_benchmark_single_row_write(benchmark):
    n_rows = 1000
    in_kwargs = {"n_rows": n_rows} 

    result = benchmark.pedantic(single_write_of_rows, kwargs=in_kwargs, 
                                iterations=1, 
                                rounds=1)
    assert result == n_rows


@pytest.mark.skip
def test_benchmark_multiple_row_write(benchmark):
    n_rows = 1000
    in_kwargs = {"n_rows": n_rows} 

    result = benchmark.pedantic(multiple_write_of_rows, kwargs=in_kwargs, 
                                iterations=1, 
                                rounds=1)

    assert result == n_rows


if __name__ == "__main__":
    test_read_and_write_table()
