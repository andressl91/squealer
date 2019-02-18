from typing import Dict
from io import StringIO

from squealer.sql_datatypes import SqlDataType
from squealer.sql_table import DataTable
from squealer.sqlite_session import SqliteSession


class DataBase:

    def __init__(self, *, db_path: str):
        """Toolbox for performing sql queries to database.

        Parameters:
            sql_session:

        """
        self._sql_local_session = SqliteSession(db_path=db_path)
        self._sql_memory_session = None
        self._sql_active_session = self._sql_local_session
        self._context = "local"
        self.tables = {}
        self.build_db()

    def _validate_category_type(self, categories) -> bool:
        """Check for valid sql datatype using SqlDatType enum.

        Paramters:
            categories: Map between column name and data type.

        Returns:
            True if all categories has valid sql data types.

        """
        valid_types = SqlDataType.data_types()
        for cat, data_type in categories.items():
            d_type = data_type.split(" ")[0]
            if d_type not in valid_types:
                raise TypeError(f"""Category "{cat}" of type "{data_type}".
                                Not Valid data_type""")

        return True

    def _fetch_all_tables(self):
        with self._sql_active_session as sql_ses:
            sql_ses.cursor.execute("SELECT name FROM sqlite_master where type='table'")

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
        with self._sql_active_session as sql_ses:
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
        self.tables = {}
        #TODO: Smarter update of available tables
        for tab in tables:
            column_data_type = self.pragma_table(table_name=tab)
            data_table = DataTable(sql_session=self._sql_active_session,
                                   table_name=tab,
                                   categories=column_data_type)
            # self.__dict__[tab] = data_table
            self.tables[tab] = data_table

    def create_table(self, table_name: str,
                     categories: Dict[str, str],
                     primary_key_id: bool=True,
                     overwrite: bool=False):

        """Create new table in connected database.

        Paramteters:
            data_table: User defined table.

        """
        self._validate_category_type(categories)
        with self._sql_active_session as sql_ses:
            if not self._does_table_exist(sql_ses, table_name) or overwrite:
                if primary_key_id:
                    text = f"""CREATE TABLE {table_name} (id INTEGER PRIMARY KEY"""
                    for cat, sql_type in categories.items():
                        text += f", {cat} {sql_type}"

                    text += ")"

                else:
                    text = f"""CREATE TABLE {table_name} ("""
                    keys = list(categories.keys())
                    for i in range(len(keys)):
                        if i < len(keys) - 1:
                            text += f"{keys[i]} {categories[keys[i]]}, "
                        else:
                            text += f"{keys[i]} {categories[keys[i]]}"

                    text += ")"
                (text)
                sql_ses.cursor.execute(text)
        # TODO: Possibly not needed if exist, just check dict
        self.build_db()

    def delete_table(self, table_name):
        """Remove table from database."""
        sql = f"DROP TABLE {table_name}"
        with self._sql_active_session as sql_ses:
            sql_ses.cursor.execute(sql)
            sql_ses.commit()

        # Delete from list for now
        # TODO: Change to set
        del self.tables[table_name]
        # del self.__dict__[table_name]

    def get_categories(self, table_name):
        # TODO: Must be a better way to get categories from table 
        sql = f"""SELECT * FROM {table_name}"""
        with self._sql_active_session as sql_ses:
            sql_ses.cursor.execute(sql)
            categories = list(map(lambda x: x[0], sql_ses.cursor.description))
            return categories

    def in_memory(self):
        """Temp weak check if local db in memory has been initiated by instance attribute"""
        if self._sql_memory_session is None:
            return False

        return True
    
    @property
    def memory_db(self):
        if self._sql_memory_session is None:
            self._sql_memory_session = SqliteSession(db_path=":memory:")

        return self._sql_memory_session
    
    @property
    def local_db(self):
        return self._sql_local_session

    @property
    def active_db(self):
        return self._sql_active_session

    @property
    def context(self):
        return self._context

    def set_local_session(self):
        self._context = "local"
        self._sql_active_session = self.local_db
        self.build_db()

    def set_memory_session(self):
        self._context = "memory"
        self._sql_active_session = self.memory_db
        self.build_db()

    def get_active_session(self):
        return self._sql_active_session

    def load_memory_to_local(self):
        dest = self.local_db
        source = self.memory_db.connection

        source.backup(dest.connection)
        dest.close()
        self.build_db()

    def load_to_memory_stringio(self):
        # initial_value='', newline='\n'
        tempfile = StringIO() 
        with self.local_db as sql_ses:
            for line in sql_ses.connection.iterdump():
                tempfile.write('%s\n' % line)

        tempfile.seek(0)

        with self.memory_db as sql_mem:
            sql_mem.cursor.executescript(tempfile.read())
            sql_mem.commit()
        
        if self.context != "memory":
            self.set_memory_session()
            self.build_db()
            self.set_local_session()

        else:
            self.build_db()

    def load_to_memory(self):
        source = self.local_db
        dest = self.memory_db.connection

        source.connection.backup(dest)
        source.close()

        if self.context != "memory":
            self.set_memory_session()
            self.build_db()
            self.set_local_session()

        else:
            self.build_db()


    def __truediv__(self, other):
        if other in self.tables:
            return self.tables[other] 
        return 

