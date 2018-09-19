from typing import Dict
from enum import Enum

from squealer.sqlite_session import SqlSession


class SqlDataType(Enum):
    """Declare all valid Sqlite data types"""
    null = "NULL"
    integer = "INTEGER"
    real = "REAL"
    text = "TEXT"
    blob = "BLOB"

    @classmethod
    def data_types(cls):
        """Return list of all sqlite data types"""
        return [data_type.value for data_type in cls]

class DataTable:

    def __init__(self, *, table_name: str, categories: Dict[str, str], 
                 primary_key: str=False):

        """Base class for a sql table.

       Parameters:
           table_name: Name of table in sql database.
           categories: Map between column name and data type.
           primary_key: primary_key for sql table. 

        """
        self._primary_key = primary_key
        self._table_name = table_name
        if self.validate_category_type(categories):
            self._categories = categories

    def validate_category_type(self, categories) -> bool:
        """Check for valid sql datatype using SqlDatType enum.

        Paramters:
            categories: Map between column name and data type.

        Returns:
            True if all categories has valid sql data types.

        """
        valid_types = SqlDataType.data_types()
        (valid_types)
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
        """Toolbox for performing sql queries to database.

        Parameters:
            sql_session: 

        """
        self._sql_session = sql_session

    def _does_table_exist(self, sql_ses, table_name) -> bool:
        """Check if table exists within database.
        
        Parameters:
            sql_ses: Current sql session.
            table_name: Name of the provided DataTable.

        Returns:
            True if table does exist, else False.

        """
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
        """Create new table in connected database.
        
        Paramteters:
            data_table: User defined table.

        """
        with self._sql_session as sql_ses:
            if self._does_table_exist(sql_ses, data_table.table_name):
                return
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
            (text) 
            sql_ses.cursor.execute(text)

    def write_to_table(self, data_table: DataTable, sql_data: Dict[str, str]):
        """Write data to data table.
        
        """
        #TODO: Make keyword argument to write to table even if all
        #categoies are not present. Ex beautiful soup missing entity from scan.
        if self._valid_keys(data_table, sql_data):
            with self._sql_session as sql_ses:
                text = f"INSERT INTO {data_table.table_name}"
                features = "(" + ",".join(cat for cat in sql_data) + ")"
                nr_values = "VALUES(" + ",".join("?" for i in
                                                 range(len(sql_data))) + ")"

                sql = text + features + nr_values

                values = tuple(sql_data.values())
                (sql, values)
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

    def get_categories(self, data_table: DataTable):
        #TODO: Add bool for addiing categories to DataTable if none
        sql = f"""SELECT * FROM {data_table.table_name}"""
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            categories = list(map(lambda x: x[0], sql_ses.cursor.description))
            return categories

    def select(self, data_table: DataTable, sql: str):
        #TODO: Get datatype made during construction
        sql = f"""SELECT {sql} FROM {data_table.table_name}"""
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            result = sql_ses.cursor.fetchall()
            return result

    def write_to_csv(self, path: str, table: str):
        # to export as csv file
        # WRITE TO CSV IKKE HELT GOD DA "," I ADRESSEN F* UP CSV
        # MULIG PREPROSSESEROMG I AKTIVITET ER LØSNINGEN!
        self.connect_db()
        cursor = self.get_cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        table_info = cursor.fetchall()
        colum_headers = " ".join([t[1] + "," for t in table_info])[:-1]

        with open(path, "wb") as write_file:
            write_file.write(colum_headers.encode())
            write_file.write("\n".encode())
            for row in cursor.execute(f"SELECT * FROM {table}"):
                writeRow = " ".join([str(i) + "," for i in row])[:-1]
                write_file.write(writeRow.encode())
                write_file.write("\n".encode())
        self.close_db()
