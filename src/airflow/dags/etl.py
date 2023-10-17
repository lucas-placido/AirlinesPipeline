from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import psycopg2
import pandas as pd

dag = DAG(
    dag_id='airlines',
    start_date=pendulum.datetime(
        2023, 10, 14, 6, 0, 0, tz='America/Sao_Paulo'
    ),
    schedule='0 6 * * *',
)

# Configurações do banco de dados
db_config = {
    'dbname': 'airlinesdb',
    'user': 'postgres',
    'password': 'mysecretpassword',
    'host': 'postgresdb',
    'port': '5432',
}

extract_path = '/opt/airflow/src/airlines/data/e/'
transform_path = '/opt/airflow/src/airlines/data/t/'


def extract_data(output_file, table_name):
    connection = None  # Inicializa a variável connection

    # Nome da tabela e consulta SQL
    query = f'SELECT * FROM {table_name}'

    try:
        # Conecta ao banco de dados
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Executa a consulta
        cursor.execute(query)

        # Pega os nomes das colunas
        column_names = [desc[0] for desc in cursor.description]

        # Abre o arquivo de saída para escrita
        with open(output_file, 'w', encoding='utf-8') as file:
            # Escreva os nomes das colunas como a primeira linha
            file.write(';'.join(column_names) + '\n')

            row = cursor.fetchone()
            while row is not None:
                # Converte a linha em uma string
                row_str = ';'.join(str(col) for col in row)
                # Escreve a linha no arquivo
                file.write(row_str + '\n')

                row = cursor.fetchone()

        # Fecha o arquivo de saída e o cursor
        file.close()
        cursor.close()

        print(
            "Consulta concluída e resultados escritos em '{}'.".format(
                output_file
            )
        )

    except psycopg2.Error as e:
        print('Ocorreu um erro ao conectar ao banco de dados:', e)

    finally:
        if connection is not None:
            connection.close()


def normalize_json_column(df, col):
    df[col] = df[col].apply(lambda x: eval(x))
    model_normalized = pd.json_normalize(df[col])
    model_normalized.rename(
        columns={
            'en': f'{col}_en',
            'ru': f'{col}_ru',
        },
        inplace=True,
    )

    df.drop(col, axis=1, inplace=True)
    df = pd.concat([df, model_normalized], axis=1)

    return df

def convert_datetime_format(df, cols = []):
    for col in cols:
        df[col] = pd.to_datetime(df[col]).dt.strftime('%d/%m/%Y %H:%M:%S')
    return df


def create_fact_table():
    bookings = pd.read_csv(extract_path + 'bookings.csv', sep=';')
    tickets = pd.read_csv(extract_path + 'tickets.csv', sep=';')
    ticket_flights = pd.read_csv(extract_path + 'ticket_flights.csv', sep=';')
    boarding_passes = pd.read_csv(extract_path + 'boarding_passes.csv', sep=';')
    flights = pd.read_csv(extract_path + 'flights.csv', sep=';')

    temp = (
        bookings\
        .merge(tickets, how='left', on='book_ref')
        .merge(ticket_flights, how='left', on='ticket_no')
        .merge(boarding_passes, how='left', on='flight_id')
        .merge(flights, how='left', on='flight_id')
    )

    fact_table = temp[
        ['book_ref',
        'book_date',
        'ticket_no',
        'flight_id',
        'flight_no',
        'passenger_id',
        'amount',
        'total_amount']
    ]
    del temp
    fact_table.to_csv(transform_path + 'fact_table.csv', sep=';', index=False)
    del fact_table


def transform_aircrafts_data():
    df = pd.read_csv(extract_path + 'aircrafts_data.csv', sep=';')
    aircrafts_data = normalize_json_column(df, 'model')

    del df
    aircrafts_data.to_csv(
        transform_path + 'aircrafts_data.csv', sep=';', index=False
    )
    del aircrafts_data


def transform_airports_data():
    df = pd.read_csv(extract_path + 'airports_data.csv', sep=';')
    df = normalize_json_column(df, 'airport_name')
    airports_data = normalize_json_column(df, 'city')
    airports_data['lat'] = airports_data['coordinates'].apply(lambda x: eval(x)[0])
    airports_data['long'] = airports_data['coordinates'].apply(lambda x: eval(x)[1])
    airports_data.drop('coordinates', axis=1, inplace=True)

    del df
    airports_data.to_csv(
        transform_path + 'airports_data.csv', sep=';', index=False
    )
    del airports_data


def transform_boarding_passes():
    df = pd.read_csv(extract_path + 'boarding_passes.csv', sep=';')
    df.to_csv(transform_path + 'boarding_passes.csv', sep=';', index=False)
    del df


def transform_tickets():
    df = pd.read_csv(extract_path + 'tickets.csv', sep=';')
    tickets = normalize_json_column(df, 'contact_data')
    tickets['email'].fillna('Não informado', inplace=True)

    del df
    tickets.to_csv(transform_path + 'tickets.csv', sep=';', index=False)
    del tickets


def transform_bookings():
    pass


def transform_flights():
    flights = pd.read_csv(extract_path + 'flights.csv', sep=';')
    convert_datetime_format(flights, ['scheduled_departure', 'scheduled_arrival', 'actual_departure', 'actual_arrival'])
    flights.fillna('Não informado', inplace=True)
    flights.to_csv(transform_path + 'flights.csv', sep=';', index=False)
    del flights
    

def transform_seats():
    seats = pd.read_csv(extract_path + 'seats.csv', sep=';')
    seats.to_csv(transform_path + 'seats.csv', sep=';', index=False)
    del seats


def transform_ticket_flights():
    ticket_flights = pd.read_csv(extract_path + 'ticket_flights.csv', sep=';')
    ticket_flights.to_csv(transform_path + 'ticket_flights.csv', sep=';', index=False)
    del ticket_flights


table_names = [
    'aircrafts_data',
    'airports_data',
    'boarding_passes',
    'bookings',
    'flights',
    'seats',
    'ticket_flights',
    'tickets',
]

# Operators
for i, table in enumerate(table_names):
    extract = PythonOperator(
        task_id=f'extract_{table}',
        dag=dag,
        python_callable=extract_data,
        op_args=[f'{extract_path}{table}.csv', f'{table}'],
    )

create_fact = PythonOperator(
    task_id='create_fact_table',
    python_callable=create_fact_table,
    dag=dag,
)

t_tickets = PythonOperator(
    task_id='transform_tickets',
    python_callable=transform_tickets,
    dag=dag,
)

t_aircrafts = PythonOperator(
    task_id='transform_aircrafts_data',
    python_callable=transform_aircrafts_data,
    dag=dag,
)

t_airports_name = PythonOperator(
    task_id='transform_airports_data',
    python_callable=transform_airports_data,
    dag=dag,
)

t_flights = PythonOperator(
    task_id='transform_flights',
    python_callable=transform_flights,
    dag=dag,
)

t_seats = PythonOperator(
    task_id='transform_seats',
    python_callable=transform_seats,
    dag=dag,
)

t_ticket_flights = PythonOperator(
    task_id='transform_ticket_flights',
    python_callable=transform_ticket_flights,
    dag=dag,
)

t_boarding_passes = PythonOperator(
    task_id='transform_boarding_passes',
    python_callable=transform_boarding_passes,
    dag=dag,
)

[extract] >> create_fact >> [t_tickets, t_aircrafts, t_airports_name, t_flights, t_seats, t_ticket_flights, t_boarding_passes]
