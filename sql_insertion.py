# src/mysql/sql_insertion.py
__version__ = '1.1.5'

from sqlite3 import connect as connect_sqlite, OperationalError
from mysql.connector import connect, Error 
from json import  load, JSONDecodeError
from src.log.log import LoggerSetup
from sys import path

#Add the path for the MySQL configuration if not already present
if r'V:\00_CONF_ROBOS\MYSQL\Conection' not in path:
    path.append(r'V:\00_CONF_ROBOS\MYSQL\Conection')

#Path to the MySQL configuration JSON file
json_file_path = r'DIRECTORY WHERE YOUR FILE CONFIG IS LOCATED, CASE NECESSARY'

class InsertSQL:
    def __init__(self, query: str = None):
        # Initialize the class with an optional SQL query
        self.query = query
        self.__log = LoggerSetup(
            r'DIRECTORY WHERE YOUR MUST SAVE FILE LOG',  # Network path to log directory
            'NAME_LOG.log',  # Log file name
            'NAME/CATEGORY'  # Log process name/category
        )
        self._sqlite_conn = None

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
                # Create a MySQL connection using the provided parameters
                self.__connection = connect(**self._connection_info)
                return self.__connection 
            
            except Error as e:
                # Handle MySQL connection errors
                self.__log.error_message(f"MySQL query execution error: {e}")
                # Rollback the transaction in case of an error
                self._connection.rollback()
                #return None

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
            finally:
                # Close the connection
                self._connection.close()

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

            finally:
                self._connection.close()

    def delete(self, host: str = None, port: str = None, user: str = None, password: str = None, database: str = None) -> None:
        """
            Executes a delete query on the MySQL database.
        """
        self._connection = self.connection(host, port, user, password, database)

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
                
            finally:
                #Close the connection
                self._connection.close()
    
    def connect_sqlite(self, path_dataBase):
        try:
            """
                Estabelece uma conexão persistente com o banco de dados.
            """
            if not self._sqlite_conn:
                self._sqlite_conn = connect_sqlite(path_dataBase)
                self._sqlite_cursor = self._sqlite_conn.cursor()

        except OperationalError as e:
            self.__log.error_message(f"SQLite connection error: {e}")

    def sql_lite_insert(self, path_dataBase: str = None, values: str = None, create_base_save_process: str = None):
        """
            Inserts data into an SQLite database.
            
            Parameters:
                path_dataBase (str): Path to the SQLite database file.
                values (str or list): Values to be inserted. Can be a single tuple or a list of tuples.
                create_base_save_process (str): Optional SQL statement to create the table before inserting data.
        """
        #Connect to the SQLite database
        self.connect_sqlite(path_dataBase)
        
        if self._sqlite_conn:
            try:
                if isinstance(values, list) or isinstance(values, tuple):
                    if create_base_save_process:
                        #If the table doens't exist, it will go create a table specified in the function argument  
                        self._sqlite_cursor.execute(create_base_save_process)

                    if values is not None and len(values) > 0:
                        #Insert multiple rows into the table
                        self._sqlite_cursor.executemany(self.query, values)

                    else:
                        self._sqlite_cursor.execute(values)

                else:
                    if create_base_save_process:
                        #Execute table creation statement if provided
                        self._sqlite_cursor.execute(create_base_save_process)
                    
                    #Insert a single row into the table
                    self._sqlite_cursor.execute(self.query, values)

                #Commit the transaction only for UPDATE, INSERT, DELETE
                if self.query.strip().upper().startswith(("INSERT")):
                    #Commit the transaction to save changes
                    self._sqlite_conn.commit()

                else:
                    #If the value of self.query doesn't contain 'INSERT', it is going to write an info message in the log file.
                    self.__log.info_message(f"SQLite without connection: {e}")

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
        
        if self._sqlite_conn:
            try:
                # If values are provided, execute the query with those values
                if values is not None:
                    self._sqlite_cursor.executemany(self.query, values)
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
                    for value in self._dados_sqlite:
                        self._lista_values.append(value)

                    return self._lista_values
            
            except OperationalError as e:
                # Log any errors that occur during query execution
                self.__log.error_message(f"SQLite query execution error: {e}")
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
                self.__log.error_message(f"SQLite query execution error: {e}")
