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
        if self._connection:
            return
        self._connection = sqlite3.connect(self.db_path)

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self._connection.cursor()
        return self._cursor

    def commit(self):
        self._connection.commit()

    def execute(self, sql_command):
        self._cursor.execute(sql_command)

    def close_db(self):
        """Closes the connection:

        Note:
            Due too Python API PEP 249, connections is unusable after close().
            Therefore create new connection each time.
        """
        self._connection.close()
        self._connection = None
        self._cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        """
        Note:
            If one uses 2 or more with statements regarding same connection,
            the close_db will make connection unusable. Tempfix, attemt to
            connect and then close.
        """
        self.connect()
        self.close_db()


