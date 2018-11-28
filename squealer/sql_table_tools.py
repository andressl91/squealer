from typing import List, Dict
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

    def __init__(self, *, sql_session, table_name: str, categories: Dict[str, str],
                 primary_key: str=False):

        """Base class for a sql table.

       Parameters:
           table_name: Name of table in sql database.
           categories: Map between column name and data type.
           primary_key: primary_key for sql table.

        """
        self._sql_session = sql_session
        self._primary_key = primary_key
        self._table_name = table_name
        if self.validate_category_type(categories):
            self._categories = categories

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def categories(self):
        return self._categories

    @property
    def table_name(self):
        return self._table_name

    def _valid_keys(self, sql_data: Dict[str, str]):
        no_id_categoires = [cat for cat in self._categories if cat != "id"]
        if set(no_id_categoires) == set(sql_data.keys()):
            return True

        else:
            raise RuntimeError("Some keys are invalid!")

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

    def select(self, sql: str):
        """User defined sql select with fetchall"""
        # TODO: Get datatype made during construction
        sql = f"""SELECT {sql} FROM {self._table_name}"""
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            result = sql_ses.cursor.fetchall()
            return result

    def multiselect(self):
        pass
   
    def clean_table(self):
        """Remove all values in table. """
        sql = f"DELETE FROM {self._table_name}"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()

    def write_to_table(self, sql_data: Dict[str, str]):
        """Write data to data table.


        Note:
            For missing data use NULL as value.

        """
        if self._valid_keys(sql_data):
            with self._sql_session as sql_ses:
                text = f"INSERT INTO {self._table_name}"
                features = "(" + ",".join(cat for cat in sql_data) + ")"
                nr_values = "VALUES(" + ",".join("?" for i in
                                                 range(len(sql_data))) + ")"

                sql = text + features + nr_values

                values = tuple(sql_data.values())
                (sql, values)
                sql_ses.cursor.execute(sql, values)
                sql_ses.commit()

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


class DataTableTools:

    def __init__(self, *, sql_session: SqlSession):
        """Toolbox for performing sql queries to database.

        Parameters:
            sql_session:

        """
        self._sql_session = sql_session
        self.tables = {}

    def _validate_category_type(self, categories) -> bool:
        """Check for valid sql datatype using SqlDatType enum.

        Paramters:
            categories: Map between column name and data type.

        Returns:
            True if all categories has valid sql data types.

        """
        valid_types = SqlDataType.data_types()
        for cat, data_type in categories.items():
            if data_type not in valid_types:
                raise TypeError(f"""Category "{cat}" of type "{data_type}". Not Valid data_type""")

        return True
   
    def _fetch_all_tables(self):
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute("SELECT name FROM sqlite_master where \
                                  type='table'")

            tables = sql_ses.cursor.fetchall()

        return [tab[0] for tab in tables]

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
    
    def pragma_table(self, table_name):
        column_category_map = {}
        sql = f"PRAGMA table_info({table_name})"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            pragma_info = sql_ses.cursor.fetchall()

        for prag in pragma_info:
            if prag:
                column_category_map[prag[1]] = prag[2]

        return column_category_map

    def build_db(self):
        """Mirror all tables in sqlite db with DataTable objects.

        Note:
            For now all tables within database is added so instance __dict__,
            and tables list.

        """
        tables = self._fetch_all_tables()
        for tab in tables:
            column_data_type = self.pragma_table(table_name=tab)
            data_table = DataTable(sql_session=self._sql_session,
                                   table_name=tab,
                                   categories=column_data_type)
            self.__dict__[tab] = data_table
            self.tables[tab] = data_table


    def create_table(self, table_name: str,
                           categories: Dict[str, str],
                           primary_key=False):
        """Create new table in connected database.

        Paramteters:
            data_table: User defined table.

        """
        self._validate_category_type(categories)
        with self._sql_session as sql_ses:
            if self._does_table_exist(sql_ses, table_name):
                return
            if not primary_key:
                text = f"""CREATE TABLE {table_name} (id INTEGER PRIMARY KEY"""
                for cat, sql_type in categories.items():
                    text += f", {cat} {sql_type}"

                text += ")"

            else:
                text = f"""CREATE TABLE {table_name} ("""
                for cat, sql_type in categories.items():
                    text += f"{cat} {sql_type},"

                text += "PRIMARY KEY (primary_key))"
            (text)
            sql_ses.cursor.execute(text)
        self.build_db()

        # For now best way to update instance attribute and __dict__.


    def delete_table(self, table_name):
        """Remove table from database."""
        sql = f"DROP TABLE {table_name}"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()

        # Delete from list for now
        # TODO: Change to set
        del self.tables[table_name]
        del self.__dict__[table_name]

    def get_categories(self, table_name):
        # TODO: Add bool for addiing categories to DataTable if none
        sql = f"""SELECT * FROM {table_name}"""
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            categories = list(map(lambda x: x[0], sql_ses.cursor.description))
            return categories


