import pandas as pd
from transform_data import Transform
import os

t = Transform()

read_folder = os.getenv("extract_path")
save_folder = os.getenv("transform_path")
table_name = 'bookings.csv'


read_file = read_folder + table_name
write_file = save_folder + table_name

df = pd.read_csv(read_file)
df['book_date'] = t.datetime_converter(df['book_date'])

df.to_csv(write_file, index=False)