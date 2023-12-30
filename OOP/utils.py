import pypyodbc
import logging
import configparser
import time
import pandas as pd
import query as query

class Utils:
    def __init__(self, Driver, Server, Database):
        self.driver = Driver
        self.server = Server
        self.database = Database

    def retry_on_failure(max_retries, delay):
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                for i in range(1, max_retries + 1):
                    try:
                        print(f'Connecting to the database  attempt {i}')
                        result = func(self, *args, **kwargs)
                        print(f'Connected to the database on attempt {i} successfully')
                        return result  # Return the result here
                    except Exception as e:
                        print(f'The exception occurred is {e}, the error is {e}')
                        time.sleep(delay)
                raise Exception("Failed to connect to database after multiple attempts")
            return wrapper
        return decorator

    @retry_on_failure(5, 3)
    def get_data(self, table):
        connection_string = f'Driver={{{self.driver}}};SERVER={{{self.server}}};DATABASE={{{self.database}}};Trusted_Connection=Yes;' # Windows Authentication (Trusted_Connection=True)
        conn = pypyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP (10) * FROM [Pratices Appcube].{table}")
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        # Display the Pandas Dataframe
        print(df)
        cursor.close()
        conn.close()
        return df  
    @retry_on_failure(5,3)
    def log_to_database(self, level, message):
        connection_string = f'Driver={{{self.driver}}};SERVER={{{self.server}}};DATABASE={{{self.database}}};Trusted_Connection=Yes;' # Windows Authentication (Trusted_Connection=True)
        conn = pypyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(query.insert_logging,(level,message))
        cursor.close()
        conn.close()
    
    def log_info(self, message):
        self.log_to_database("info", message)
    def log_error(self, message):
        self.log_to_database("error", message)
        
        
    def insert_data(self,dest_table, source_table):
        values = self.get_data(source_table)
        connection_string = f'Driver={{{self.driver}}};SERVER={{{self.server}}};DATABASE={{{self.database}}};Trusted_Connection=Yes;' # Windows Authentication (Trusted_Connection=True)
        conn = pypyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(f"""Truncate table {dest_table}""")
        cursor.executemany(f"""Insert into {dest_table} ([Customer],[Aquarium],[Size (gallons)], [CreatedDate]) VALUES (?,?,?,getdate())""", values.values.tolist())
        current_time = time.time()
        self.log_info(f"Insert into {dest_table} successfully at {current_time}")
        cursor.close()
        conn.close()
        
        

# Create an instance of the Utils class
a = Utils("SQL Server", Server="DESKTOP-GMJ83SF", Database="Pratices Appcube")


a.insert_data("[dbo].[OLE DB Destination]","[dbo].[Cust Destination]")
# a.log_info("Can not insert")
 # Example: Try to connect to the database
# try:
#     result = a.get_data("dbo.[Cust Dest2]")
#     print(result)
# except Exception as e:
#     print(f'Error connecting to the database: {e}')
