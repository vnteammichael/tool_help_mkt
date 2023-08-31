import mysql.connector
import pandas as pd

class MySQLConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def insert_dataframe(self, table, dataframe, igorne = False, on_duplicate = False):
        try:
            if not self.connection:
                self.connect()

            dataframe_columns = list(dataframe.columns)
            placeholders = ', '.join(['%s'] * len(dataframe_columns))

            query = f"INSERT "
            query += " IGNORE "  if igorne else ""  
            query += f" INTO {table} ({', '.join(dataframe_columns)}) VALUES ({placeholders})"

            
            cursor = self.connection.cursor()
            for row in dataframe.itertuples(index=False):
                cursor.execute(query, row)
            self.connection.commit()
        except mysql.connector.Error as err:
            print("Error:", err)
        finally: 
            cursor.close()
    
    def delete_data_by_condition(self, table, condition_dict):
        try:
            if not self.connection:
                self.connect()

            cursor = self.connection.cursor()
            placeholders = ', '.join([f'{key} = %s' for key in condition_dict.keys()])
            values = tuple(condition_dict.values())
            query = f"DELETE FROM {table} WHERE {placeholders}"
            cursor.execute(query, values)
            self.connection.commit()
        except mysql.connector.Error as err:
            print("Error:", err)
        finally: 
            cursor.close()
        

    def insert_detail(self, table, detail):
        try:
            if not self.connection:
                self.connect()

            dataframe_columns = list(detail.keys())
            placeholders = ', '.join(['%s'] * len(dataframe_columns))

            query = f"INSERT IGNORE  INTO {table} ({', '.join(dataframe_columns)}) VALUES ({placeholders})"
            
            cursor = self.connection.cursor()
            cursor.execute(query, list(detail.values()))
            self.connection.commit()
        except mysql.connector.Error as err:
            print("Error:", err)
        finally: 
            cursor.close()


    def update_data(self,dataframe, table_name,update_cols,condition_cols):
        try:
            if not self.connection:
                self.connect()
            cursor = self.connection.cursor()
            for index, row in dataframe.iterrows():
                placeholders = ', '.join([f"{col} = '{row[col]}'" for col in update_cols])
                conditions = ' and '.join([f"{col} = '{row[col]}'" for col in condition_cols])
                update_query = f"UPDATE {table_name} SET {placeholders} WHERE {conditions}"
                cursor.execute(update_query)
            self.connection.commit()    
        except mysql.connector.Error as err:
            print("Error:", err)
        finally: 
            cursor.close()

    def query_data_to_dataframe(self,query):
        try:
            if not self.connection:
                self.connect()
            result_df = pd.read_sql_query(query, self.connection)
            return result_df
        except mysql.connector.Error as err:
            print("Error:", err)
    

    def close(self):
        if self.connection:
            self.connection.close()