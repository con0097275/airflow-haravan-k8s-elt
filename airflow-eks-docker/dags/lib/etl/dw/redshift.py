import psycopg2
from typing import Iterator
from contextlib import contextmanager


##simple case for Redshift warehouse only
class DatawarehouseConnection:
    """
    A class to manage Redshift database connections with context management support.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        """
        Initialize the connection parameters.

        Args:
            host (str): Redshift host address.
            port (int): Redshift port number.
            database (str): Redshift database name.
            user (str): Username for Redshift.
            password (str): Password for Redshift.
        """
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._connection = None
 
    def connect(self) -> None:
        """
        Establish a connection to Redshift.
        """
        self._connection = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password
        )
        print("Connected to Redshift.")

    def close(self) -> None:
        """
        Close the Redshift connection.
        """
        if self._connection:
            self._connection.close()
            print("Connection to Redshift closed.")

    @contextmanager
    def managed_cursor(self) -> Iterator[psycopg2.extensions.cursor]:
        """
        Provide a managed cursor for database operations.
        
        Yields:
            psycopg2.extensions.cursor: A cursor for Redshift operations.
        """
        try:
            if not self._connection:
                self.connect()
            cursor = self._connection.cursor()
            yield cursor
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            print(f"Error during database operation: {e}")
            raise
        finally:
            cursor.close()

    def __enter__(self) -> "DatawarehouseConnection":
        """
        Enter the runtime context related to the connection.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the runtime context related to the connection.
        """
        self.close()