#src/mysql/sql_insertion.py

import mysql.connector
import json
import sys

if r'V:\00_CONF_ROBOS\MYSQL\Conection' not in sys.path:
     sys.path.append(r'V:\00_CONF_ROBOS\MYSQL\Conection')

json_file_path = r'V:\00_CONF_ROBOS\MYSQL\Conection\mysql_config.json'

class InsertSQL:
    def __init__(self, query):
        self.query = query

    def read_connection_info(self, filename):
        """
        Read connection information from JSON file.
        """
        try:
            with open(filename, 'r') as rJSON:
                connection_info = json.load(rJSON)
            return connection_info
        except json.JSONDecodeError as e:
            print(f"JSON Decode error: {e}")
            return None

    def connection(self, host, port, user, password, database):
        """
        Establishes a connection to the MySQL database.
        """

        self._connection_info =  self.read_connection_info(json_file_path)

        self._connection_info = {
            'host': self._connection_info[host],
            'port': self._connection_info[port],
            'user': self._connection_info[user],
            'password': self._connection_info[password],
            'database': self._connection_info[database]
        }

        if self._connection_info:
            try:
                connection = mysql.connector.connect(**self._connection_info)
                return connection
            except mysql.connector.Error as e:
                print(f"MySQL connection error: {e}")
                return None

    def mysql_insert(self, host, port, user, password, database):
        """
        Executes a MySQL query.
        """

        connection = self.connection(host, port, user, password, database)
        if connection:

            try:
                cursor = connection.cursor()
                # if values:
                cursor.execute(self.query)
                # else:
                #     cursor.execute(self.query)

                #result = cursor.fetchall()
                connection.commit()

                # results in result: print(results)
            except mysql.connector.Error as e:
                print(f"MySQL query execution error: {e}")
                pass
            finally:
                connection.close()

    def mysql_query(self, host, port, user, password, database):
        self._connection = self.connection(host, port, user, password, database)

        if self._connection:
            try:
                self._cursor = self._connection.cursor()

                self._cursor.execute(self.query)

                self._cursor = self._cursor.fetchall()
                self._connection.commit()

                self.list_result = []
                for result in self._cursor:
                    self.list_result.append(result)

                return self.list_result
                #results in result: print(results)
            except mysql.connector.Error as e:
                print(f"MySQL query execution error: {e}")
                pass
            finally:
                self._connection.close()