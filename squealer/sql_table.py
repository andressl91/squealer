import sqlite3

from typing import Dict, List

from squealer.sql_datatypes import SqlDataType


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
        self.columns = list(self._categories.keys())

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
        for d_type in list(categories.values()):
            if d_type not in valid_types:
                raise RuntimeError("Not Valid data_type")

        return True

    def select(self, sql: List[str]):
        """Fetch one/multiple columns from table

        Attr:
            sql: List of requested columns

        """
        sql_request = f"""SELECT {" ".join(i for i in sql)} FROM
        {self._table_name}"""

        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql_request)
            result = sql_ses.cursor.fetchall()
            return result

    def select_row(self, sql: List[str]):
        """Fetch one/multiple columns from table using sqlite3 row factory

        Attr:
            sql: List of requested columns

        """
        sql_request = f"""SELECT {" ".join(i for i in sql)} FROM
        {self._table_name}"""

        with self._sql_session as sql_ses:
            sql_ses.row_factory = sqlite3.Row
            cursor = sql_ses.cursor
            cursor.execute(sql_request)

            result = sql_ses.cursor.fetchone()#fetchall()
            return result

    def clean_table(self):
        """Remove all values in table. """
        sql = f"DELETE FROM {self._table_name}"
        with self._sql_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()

    def write(self, sql_data: Dict[str, str]):
        """Write row of datato table.

        Attrs:
            sql_data: Mapping column to value

        Note:
            For missing data use NULL as value.

        """
        if self._valid_keys(sql_data):
            with self._sql_session as sql_ses:
                text = f"INSERT INTO {self._table_name}"
                features = "(" + ",".join(cat for cat in sql_data) + ")"
                nr_values = "VALUES(" + ",".join("?" * len(sql_data)) + ")"

                sql = text + features + nr_values
                values = tuple(sql_data.values())
                (sql, values)
                sql_ses.cursor.execute(sql, values)
                sql_ses.commit()

    def multi_write(self, sql_data: List[Dict[str, str]]):
        """Write multiple rows to table

        Attrs:
            sql_data: List of dicts, mapping column to value.

        Note:
            Multi write supports random order of dict, with penalty since
            every sql_data map in list must be checked. Users responsibility?

        """
        first_features = [cat for cat in sql_data[0]]
        values = []
        with self._sql_session as sql_ses:
            text = f"INSERT INTO {self._table_name}"
            features = "(" + ",".join(cat for cat in sql_data[0]) + ") "
            nr_values = "VALUES (" + ",".join("?" * len(sql_data[0])) + ")"

            sql = text + features + nr_values
            for data_values in sql_data:
                values.append(tuple(data_values[key]
                                    for key in first_features))
            sql_ses.cursor.executemany(sql, values)
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

    def __truediv__(self, other):
        if type(other) is str:
            if other in self.columns or other == "*":
                return self.select([other])