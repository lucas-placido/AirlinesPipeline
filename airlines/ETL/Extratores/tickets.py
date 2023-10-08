from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "tickets.csv"
path = save_folder + table_name

renamed_columns = "ticket_no,book_ref,passenger_id,passenger_name,contact_data"
query = """
SELECT ticket_no, book_ref, passenger_id, passenger_name, contact_data FROM tickets;
"""

tickets = airlines_db.extract(query)
airlines_db.save_as_csv(tickets, output_path=path, columns=renamed_columns)
