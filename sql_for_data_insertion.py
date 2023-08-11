from win_quality.constant.data_base import *
from win_quality.logger import logging
import mysql.connector as connection
import pandas as pd
import numpy as np

class MySqlConnection:
    def __init__(self, host: str = SQL_URL_KEY, user:str = SQL_USER, password: str = SQL_PASSWORD):
        self.host = host
        self.user_name = user
        self.password = password
        self.db_connection = None
        self.db_cursor = None

    def connect(self):
        try:
            self.db_connection = connection.connect(
                host=self.host,
                user=self.user_name,
                passwd=self.password,
                use_pure=True
            )
            self.db_cursor = self.db_connection.cursor()
            print("Connection Established with MySQL...!")
        except Exception as e:
            print(str(e))

    def create_database(self, db_name:str = DATABASE_NAME):
        try:
            self.db_cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in  self.db_cursor.fetchall()]
            
            if db_name in databases:
                logging.info(f"Database '{db_name} already exists.'")
            else:
                self.db_cursor.execute(f"CREATE DATABASE {db_name}")
                logging.info(f"Database  '{db_name}' created successfully.")

        except Exception as e:
            logging.info(str(e))

    def use_database(self,db_name:str = DATABASE_NAME):
        try:
            self.db_cursor.execute(f"USE {db_name}")
            logging.info(f"Using Database '{db_name}'  ")
            
        except Exception as e:
            logging.info(str(e))
        
    def create_table(self, table_name: str = "wine_quality_table", db_name: str = DATABASE_NAME, path: str = None):
        try:
            df = pd.read_csv(path)

            # Let's Generate the columns for creating the table
            columns = df.columns.tolist()
            column_types = []

            for col in columns:
                dtype = df[col].dtype
                if np.issubdtype(dtype, np.integer):
                    column_types.append(f"`{col}` INT")
                elif np.issubdtype(dtype, np.floating):
                    column_types.append(f"`{col}` FLOAT")
                else:
                    column_types.append(f"`{col}` VARCHAR(255)")

            # Check if table exists
            check_table_query = f"SELECT 1 FROM `{table_name}` LIMIT 1"
            self.db_cursor.execute(check_table_query)
            table_exists = self.db_cursor.fetchone()

            if table_exists:
                logging.info(f"Table '{table_name}' already exists in this '{db_name}' database.")
            else:
                # Let's write the Query for creating table
                create_table_query = f"CREATE TABLE `{table_name}` ({','.join(column_types)})"
                # print(f"{create_table_query}")  # Print the SQL query for debugging
                self.db_cursor.execute(create_table_query)

                logging.info(f"Table created successfully: {table_name} in this {db_name}...")
                # print(f"Table created successfully: {table_name} in this {db_name}...")

        except Exception as e:
            logging.info(str(e))
    
    def insert_record(self, table_name:str=COLLECTION_NAME,value=None):
        try:
            query_for_insertion = f"INSERT INTO `{table_name}` VALUES ({value})"
            self.db_cursor.execute(query_for_insertion)
            self.db_connection.commit()
            logging.info(f"Record inserted successfully..! :\n{value}")

        except Exception as e:
            self.db_connection.rollback()
            logging.info(str(e))

    def insert_csv_data(self, table_name: str = COLLECTION_NAME, csv_path: str = None):
        try:
            df = pd.read_csv(csv_path)
            values_list = df.values.tolist()

            query_for_insertion = f"INSERT INTO `{table_name}` VALUES ({','.join(['%s'] * len(df.columns))})"
            print(query_for_insertion)
            self.db_cursor.executemany(query_for_insertion, values_list)
            self.db_connection.commit()

            logging.info(f"{len(values_list)} records inserted successfully..!")

        except Exception as e:
            self.db_connection.rollback()
            logging.info(str(e))

    def fetch_all_records(self, table_name: str = COLLECTION_NAME):
        try:
            query = f"SELECT * FROM `{table_name}`"
            self.db_cursor.execute(query)
            records = self.db_cursor.fetchall()

            for record in records:
                print(record)
                logging.info(record)

        except Exception as e:
            logging.info(str(e))
    
    def list_databases(self):
        try:
            self.db_cursor.execute("SHOW DATABASES")
            databases = self.db_cursor.fetchall()
            database_names = [db[0] for db in databases]
            logging.info("List of databases:")
            for db_name in database_names:
                logging.info(db_name)
        except Exception as e:
            logging.info(str(e))
    
    def delete_all_records(self,table_name):
        try:
            self.db_cursor.execute(f"DELETE FROM {table_name}")
            self.db_connection.commit()
            logging.info("All records deleted successfully..! (; )")
        
        except Exception as e:
            self.db_connection.rollback()
            logging.inf(str(e))
    
    def delete_single_or_mul_db(self, db_name:str=None):
        try:
            if db_name:
                self.db_cursor.execute(f"DROP DATABASE {db_name}")
                logging.info(f"Database '{db_name} deleted successfully..(; ")

            else:
                self.db_cursor.execute("SHOW DATABASES")
                databases = self.db_cursor.fetchall()
                database_name = [db[0] for db in databases if db[0] not in ['information_schema', 'mysql', 'performance_schema']]

                for db_name in database_name:
                    self.db_cursor.execute(f"DROP DATABASE {db_name}")
                    logging.info(f"{'==' * 20}\nDatabase  deleted successfully...(;")
                    logging.info(f"{db_name}\n{'==' * 20}")
        except Exception as e:
            logging.info(f"Error: {str(e)}")

    



