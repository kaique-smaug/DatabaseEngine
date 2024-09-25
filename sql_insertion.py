# src/mysql/sql_insertion.py
__version__ = '1.1.4'

import mysql.connector
import json
import sys

import sqlite3 as sq

# Add the path for the MySQL configuration if not already present
if r'V:\00_CONF_ROBOS\MYSQL\Conection' not in sys.path:
    sys.path.append(r'V:\00_CONF_ROBOS\MYSQL\Conection')

# Path to the MySQL configuration JSON file
json_file_path = r'V:\00_CONF_ROBOS\MYSQL\Conection\mysql_config.json'

class InsertSQL:
    def __init__(self, query: str = None):
        # Initialize the class with an optional SQL query
        self.query = query
        
    def read_connection_info(self, filename: str = None) -> str:
        """
        Read connection information from a JSON file.
        """
        try:
            # Open the JSON file and load the connection information
            with open(filename, 'r') as rJSON:
                connection_info = json.load(rJSON)
            return connection_info
        
        except json.JSONDecodeError as e:
            # Handle JSON decoding errors
            print(f"JSON Decode error: {e}")
            return None

    def connection(self, host: str = None, port: str = None, user: str = None, password: str = None, database: str = None) -> str:
        """
        Establishes a connection to the MySQL database using information from the JSON file.
        """
        # Read the connection information from the JSON file
        self._connection_info = self.read_connection_info(json_file_path)

        # Prepare the connection parameters from the JSON data
        self._connection_info = {
            'host': self._connection_info[host],
            'port': self._connection_info[port],
            'user': self._connection_info[user],
            'password': self._connection_info[password],
            'database': self._connection_info[database]
        }

        if self._connection_info:
            try:
                # Create a MySQL connection using the provided parameters
                connection = mysql.connector.connect(**self._connection_info)
                return connection
            except mysql.connector.Error as e:
                # Handle MySQL connection errors
                print(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
                return None

    def mysql_insert(self, host: str = None, port: str = None, user: str = None, password: str = None, database: str = None, value: str = None) -> None:
        """
        Executes a MySQL insert query.
        """
        self._connection = self.connection(host, port, user, password, database)
        if self._connection:
            try:
                cursor = self._connection.cursor()
                if value is not None:
                    if len(value) > 2:
                        # Execute multiple inserts if the value is a list
                        cursor.executemany(self.query, value)
                else:
                    # Execute a single insert if no value is provided
                    cursor.execute(self.query)

                # Commit the transaction to save the changes
                self._connection.commit()
            
            except mysql.connector.Error as e:
                # Handle MySQL query execution errors
                print(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
               
            finally:
                # Close the connection
                self._connection.close()

    def mysql_query(self, host: str = None, port: str = None, user: str = None, password: str = None, database: str = None,
                                values: str = None) -> tuple[list, str]:
        """
        Executes a MySQL query with optional variable substitution and fetches all results.
        """
        self._connection = self.connection(host, port, user, password, database)

        if self._connection:
            try:
                self._cursor = self._connection.cursor()
                # Execute the query with the provided values
                if values:
                    self._cursor.execute(self.query, (values,))
                else:
                    self._cursor.execute(self.query)
                
                # Fetch all results
                self.list_result = self._cursor.fetchall()
                # Commit the transaction
                self._connection.commit()

                # Return the results and the cursor
                return self.list_result, self._cursor
                
            except mysql.connector.Error as e:
                # Handle MySQL query execution errors
                print(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
            finally:
                # Close the connection
                self._connection.close()

    def delete(self, host: str = None, port: str = None, user: str = None, password: str = None, database: str = None) -> None:
        """
        Executes a delete query on the MySQL database.
        """
        self._connection = self.connection(host, port, user, password, database)

        if self._connection:
            try:
                self._cursor = self._connection.cursor()
                # Execute the delete query
                self._cursor.execute(self.query)
                # Commit the transaction
                self._connection.commit()

            except mysql.connector.Error as e:
                # Handle MySQL query execution errors
                print(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
                
            finally:
                # Close the connection
                self._connection.close()
    
    def sql_lite_insert(self, values: str = None):
        """
        Inserts data into an SQLite database.
        """
        # Connect to the SQLite database
        self._sqlite_conn = sq.connect(r'\\canada\mis_interno\Kaique\Robos\Neon_Disc\filesImported.db')
        self._sqlite_cursor = self._sqlite_conn.cursor()

        if self._sqlite_conn:
            try:
                if isinstance(values, list):
                    # Insert multiple rows if 'values' is a list
                    self._sqlite_cursor.executemany(self.query, values)
                else:
                    # Insert a single row if 'values' is not a list
                    self._sqlite_cursor.execute(self.query, values)

                # Commit the transaction to save the changes
                self._sqlite_conn.commit()
                
            except sq.OperationalError as e:
                # Handle SQLite query execution errors
                print(f"SQLite query execution error: {e}")
                # Rollback the transaction in case of an error
                self._sqlite_conn.rollback()
            finally:
                # Close the connection
                self._sqlite_conn.close()
                
    def query_sqlite(self) -> tuple[list, str]:
        """
        Executes a query on the SQLite database and returns the results.
        """
        # Connect to the SQLite database
        self._sqlite_conn = sq.connect(r'\\canada\mis_interno\Kaique\Robos\Neon_Disc\filesImported.db')
        self._sqlite_cursor = self._sqlite_conn.cursor()

        # Execute the query
        self._sqlite_cursor.execute(self.query)
        # Fetch all results
        self._dados_sqlite = self._sqlite_cursor.fetchall()

        self._lista_values = []
        for value in self._dados_sqlite:
            self._lista_values.append(value)

        return self._lista_values
