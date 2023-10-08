from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "ticket_flights.csv"
path = save_folder + table_name

renamed_columns = "ticket_no,flight_id,fare_conditions,amount"
query = """
SELECT ticket_no, flight_id, fare_conditions, amount FROM ticket_flights;
"""

ticket_flights = airlines_db.extract(query)
airlines_db.save_as_csv(ticket_flights, output_path=path, columns=renamed_columns)
