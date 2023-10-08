from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "aircrafts_data.csv"
path = save_folder + table_name

renamed_columns = "aircraft_code,model,range"
query = """
SELECT aircraft_code, model, range FROM aircrafts_data;
"""

aircrafts_data = airlines_db.extract(query)
airlines_db.save_as_csv(aircrafts_data, output_path=path, columns=renamed_columns)