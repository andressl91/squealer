import sqlite3
from abc import abstractmethod


class SqlSession:

    @abstractmethod
    def connect_db(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def cursor(self):
        pass

    @abstractmethod
    def close_db(self):
        pass


class SqliteSession(SqlSession):

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = None
        self._cursor = None

    def connect(self):
        self._connection = sqlite3.connect(self.db_path)
    
    @property
    def cursor(self):
        if not self._connection:
            raise Exception("no connection")
        if not self._cursor:
            self._cursor = self._connection.cursor()
        return self._cursor

    def commit(self):
        self._connection.commit()

    def execute(self, sql_command):
        self._cursor.execute(sql_command)

    def close_db(self):
        self._connection.close()
        self._connection = None
        self._cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close_db()


class SqlOps():

    def __init__(self, *, sql_session):
        self._sql_session = sql_session
        self._cursor = sql_session.cursor()


    def routine(self, sql_command):
        self.execute(sql_command)
        self._sql_session.commit()

   
    
