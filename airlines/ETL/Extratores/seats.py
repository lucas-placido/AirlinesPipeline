from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "seats.csv"
path = save_folder + table_name

renamed_columns = "aircraft_code,seat_no,fare_conditions"
query = """
SELECT aircraft_code, seat_no, fare_conditions FROM seats;
"""

seats = airlines_db.extract(query)
airlines_db.save_as_csv(seats, output_path=path, columns=renamed_columns)
