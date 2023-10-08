from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "airports_data.csv"
path = save_folder + table_name

renamed_columns = "airport_cod,airport_name,city,coordinates,timezone"
query = """
SELECT airport_code, airport_name, city, coordinates, timezone FROM airports_data;
"""

airports_data = airlines_db.extract(query)
airlines_db.save_as_csv(airports_data, output_path=path, columns=renamed_columns)
