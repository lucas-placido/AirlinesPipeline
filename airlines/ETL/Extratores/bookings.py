from extract_data import airlines_db
import os

save_folder = os.getenv("extract_path")
table_name = "bookings.csv"
path = save_folder + table_name

renamed_columns = "book_ref,book_date,total_amount"
query = """
SELECT book_ref, book_date, total_amount FROM bookings;
"""

bookings = airlines_db.extract(query)
airlines_db.save_as_csv(bookings, output_path=path, columns=renamed_columns)
