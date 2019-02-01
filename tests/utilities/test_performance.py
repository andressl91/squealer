import pytest 
import time
from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession



def test_read_and_write_table(get_db_tools, get_lots_of_data):
    db_tools = get_db_tools

    keys = ["a", "b", "c", "d", "e"]
    cat = ["INTEGER" for i in range(len(keys))]
    categories = dict(zip(keys, cat))
    db_tools.create_table(table_name="data",
                          categories=categories)

    # Test write, and order of write features is arbritary
    data_table = db_tools.tables["data"]
    n_rows = 100
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


if __name__ == "__main__":
    test_read_and_write_table()
