from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "flights.csv"
path = save_folder + table_name

renamed_columns = "flight_id,flight_no,scheduled_departure,scheduled_arrival,departure_airport,arrival_airport,status,aircraft_code,actual_departure,actual_arrival"
query = """
SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival FROM flights;
"""

flights = airlines_db.extract(query)
airlines_db.save_as_csv(flights, output_path=path, columns=renamed_columns)
