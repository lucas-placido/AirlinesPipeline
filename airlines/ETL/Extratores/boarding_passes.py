from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "boarding_passes.csv"
path = save_folder + table_name

renamed_columns = "ticket_no,flight_id,boarding_no,seat_no"
query = """
SELECT ticket_no, flight_id, boarding_no, seat_no FROM boarding_passes;
"""

boarding_passes = airlines_db.extract(query)
airlines_db.save_as_csv(boarding_passes, output_path=path, columns=renamed_columns)
