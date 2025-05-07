# src/mysql/sql_insertion.py
__version__ = '1.1.6'

from sqlite3 import connect as connect_sqlite, OperationalError
from mysql.connector import connect, Error 
from json import  load, JSONDecodeError
from log import LoggerSetup
from sys import path

import sqlite3 as sq

#Add the path for the MySQL configuration if not already present
if r'V:\00_CONF_ROBOS\MYSQL\Conection' not in path:
    path.append(r'V:\00_CONF_ROBOS\MYSQL\Conection')

#Path to the MySQL configuration JSON file
json_file_path = r'PATH WHERE YOUR CONFIG FILE IS LOCALIZED'

class InsertSQL:
    def __init__(self, query: str = None):
        # Initialize the class with an optional SQL query
        self.query = query
        self.__log = LoggerSetup(
            r'PATH WHERE YOU WANT TO SAVE',  # Network path to log directory
            'LOG.log',  # Log file name
            'NAME INSTANCE'  # Log process name/category
        )
        self._sqlite_conn = None
        self.__connection_mysql = None
        
    def read_connection_info(self, filename: str = None) -> str:
        """
            Read connection information from a JSON file.
        """
        try:
            # Open the JSON file and load the connection information
            with open(filename, 'r') as rJSON:
                connection_info = load(rJSON)
            return connection_info
        
        except JSONDecodeError as e:
            #Handle JSON decoding errors
            self.__log.error_message(f"JSON Decode error: {e}")
            return None

    def connection(self) -> object:
        """
            Establishes a connection to the MySQL database using information from the JSON file.
        """
        # Read the connection information from the JSON file
        self._connection_info = self.read_connection_info(json_file_path)

        # Prepare the connection parameters from the JSON data
        self._connection_info = {
            'host': self._connection_info['host_geral'],
            'port': self._connection_info['port'],
            'user': self._connection_info['user'],
            'password': self._connection_info['password'],
            'database': self._connection_info['database'],
            'connection_timeout': 1000
        }

        if self._connection_info:
            try:
                if  not self.__connection_mysql:
                    # Create a MySQL connection using the provided parameters
                    self.__connection_mysql = connect(**self._connection_info)
                    return self.__connection_mysql 

            except Error as e:
                # Handle MySQL connection errors
                self.__log.error_message(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
                return None
    
    def close_connection_mysql(self):
        if self.__connection_mysql:
            self.__connection_mysql.close()
            self.__connection_mysql = None
            print('fechou')

    def mysql_insert(self, value: list = None, value_two: list = None) -> None:
        """
            Executes a MySQL insert query with error handling.
        """ 

        self._connection = self.connection()
        if self._connection:
               
            try:
                cursor = self._connection.cursor()
                if value is not None and len(value) > 0 and value_two is not None and len(value_two) > 0:
                                                 # Execute multiple inserts if the value is a list
                    cursor.execute(self.query, (value + value_two))
                elif value is not None and len(value) > 0:
                    
                    cursor.executemany(self.query, value)
                else:
                    #Execute a single insert if no value is provided
                    cursor.execute(self.query)
                
                    # Commit the transaction to save the changes
                self._connection.commit()
            
            except Error as e:
                self.__log.error_message(f"Database error: {e}")
                self._connection.rollback()  # Rollback the transaction
                
           

    def mysql_query(self, values: str = None) -> tuple[list, str]:
        """
            Executes a MySQL query with optional variable substitution and fetches all results.
        """
        self._connection = self.connection()

        if self._connection:
            try:
                self._cursor = self._connection.cursor()

                if values is not None:
                    # Execute the query with the provided values
                    self._cursor.execute(self.query, (values,))
                else:
                    
                    self._cursor.execute(self.query)

                #Commit the transaction only for UPDATE, INSERT, DELETE
                if self.query.strip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
                    
                    self._connection.commit()
                    return None, None  # Essas operações não retornam dados

                # Fetch all results for SELECT queries
                elif self.query.strip().upper().startswith(("SELECT", 'WITH')):                   
                    self.list_result = self._cursor.fetchall()
                    return self.list_result, self._cursor

            except Error as e:
                self.__log.error_message(f"MySQL query execution error: {e}")
                self._connection.rollback()



    def delete(self) -> None:
        """
            Executes a delete query on the MySQL database.
        """
        self._connection = self.connection()

        if self._connection:
            try:
                self._cursor = self._connection.cursor()
                #Execute the delete query
                self._cursor.execute(self.query)
                #Commit the transaction
                self._connection.commit()

            except Error as e:
                #Handle MySQL query execution errors
                self.__log.error_message(f"MySQL query execution error: {e}")
                #Rollback the transaction in case of an error
                self._connection.rollback()
                
        
    
    def connect_sqlite(self, path_database):
        try:
            if not self._sqlite_conn:
                self._sqlite_conn = connect_sqlite(path_database)
                self._sqlite_cursor = self._sqlite_conn.cursor()
            else:
                self.__log.info_message(f"SQLite connection exist")

        except OperationalError as e:
            self.__log.error_message(f"SQLite connection error: {e}")

    def sql_lite_insert(self, path_database, values, create_base_save_process):
        self.connect_sqlite(path_database)
        if not self._sqlite_conn:
            self.__log.info_message(f"SQLite without connection")
            return []
        
        try:
            if create_base_save_process:
                #If the table doens't exist, it will go create a table specified in the function argument  
                self._sqlite_cursor.execute(create_base_save_process)
        

            if isinstance(values, list):
                    self._sqlite_cursor.executemany(self.query, values)
            else:
                self._sqlite_cursor.execute(self.query, values)

            if self.query.strip().upper().startswith(("INSERT")):
                self._sqlite_conn.commit()
            
            else:
                #If the value of self.query doesn't contain 'INSERT', it is going to write an info message in the log file.
                self.__log.info_message(f"SQComand isn't 'Insert'")
        
        except OperationalError as e:
                #Handle SQLite query execution errors and log them
                self.__log.error_message(f"SQLite query execution error: {e}")

    def query_sqlite(self, path_dataBase: str = None, values: tuple = None) -> tuple[list, str]:
        """
        Executes a query on the SQLite database and returns the results.
        
        Args:
            path_dataBase (str, optional): The path to the SQLite database file.
            values (tuple, optional): The values to be used in a parameterized query (for INSERT, UPDATE, DELETE).
        
        Returns:
            tuple[list, str]: A tuple containing a list of values fetched from the query result and an optional error message.
        """
        # Connect to the SQLite database
        self.connect_sqlite(path_dataBase)
        
        if not self._sqlite_conn:
            return []
        
        if self._sqlite_conn:

            try:
                # If values are provided, execute the query with those values
                if values is not None:
                    if len(values) > 1:
                        self._sqlite_cursor.executemany(self.query, values)
                    else:
                        
                        self._sqlite_cursor.execute(self.query, values)

                else:
                    # Execute the query without values
                    self._sqlite_cursor.execute(self.query)
                
                # Commit the transaction for UPDATE, INSERT, DELETE queries
                if self.query.strip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
                    self._sqlite_conn.commit()
                    
                # If the query is a SELECT or WITH query, fetch the result
                if self.query.strip().upper().startswith(("SELECT", 'WITH')):  
                    ''' 
                        fetchone() 
                        self._dados_sqlite = self._sqlite_cursor.fetchone()
                                            or 
                        fetchall()
                        self._dados_sqlite = self._sqlite_cursor.fetchall()

                        You must decide which function to use based on the query result.
                    '''
                    # Fetch the first result row
                    self._dados_sqlite = self._sqlite_cursor.fetchone()
                    
                    # Convert the result into a list of values
                    self._lista_values = []
                    if self._dados_sqlite:
                        for value in self._dados_sqlite:
                            self._lista_values.append(value)

                    return self._lista_values
            
            except OperationalError as e:
                # Log any errors that occur during query execution
                self.__log.error_message(f"SQLite query execution finction query_sqlite error: {e}")
                # Rollback the transaction in case of an error
                self._sqlite_conn.rollback()

    
    def close_connection_sqlite(self) -> None:
        # Check if there is an active SQLite connection
        if self._sqlite_conn:
            try:
                # If the connection exists, close it and set the attribute to None
                if self._sqlite_conn:
                    self._sqlite_conn.close()
                    self._sqlite_conn = None
                else:
                    # Log an info message if there is no active connection
                    self.__log.info_message(f"SQLite without connection: {e}")

            # Handle any operational errors during the connection closing
            except OperationalError as e:
                self.__log.error_message(f"SQLite query execution close connection error: {e}")
