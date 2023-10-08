import psycopg2
import pandas as pd
import os

class Extractor:
    def __init__(self, host, port, database, user, password) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    
    def extract(self, query):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        cursor.close()
        connection.close()

        return data
    
    def save_as_csv(self, data, output_path, columns):
        columns = columns.split(",")
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(output_path, index=False)

airlines_db = Extractor(
    host="localhost",
    port="8080",
    database="airlinesdb",
    user="postgres",
    password="mysecretpassword"
)

save_folder = os.getenv("extract_path")
if not os.path.exists(save_folder):
    os.mkdir(save_folder)

write_folder = os.getenv("transform_path")
if not os.path.exists(write_folder):
    os.mkdir(write_folder)