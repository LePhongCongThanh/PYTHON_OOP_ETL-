insert_logging = f"""INSERT INTO log_table (timestamp, level, message) values (getdate(),?,?);"""


