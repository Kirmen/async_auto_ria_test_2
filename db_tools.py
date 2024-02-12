import datetime
import os
import subprocess

import psycopg2


class DatabaseManager:
    def __init__(self, db_name='autoria_info', user='postgres', password='postgres', host='localhost',
                 port='5433'):
        self.conn = psycopg2.connect(database=db_name, user=user, password=password, host=host, port=port)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()


def create_db():
    with DatabaseManager() as cursor:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cars (
            id SERIAL PRIMARY KEY,
            url TEXT,
            title TEXT,
            price_usd INT,
            odometer INT,
            username TEXT,
            phone_number TEXT,
            images_urls TEXT,
            images_count INT,
            car_number TEXT,
            car_vin TEXT,
            datetime_found TIMESTAMP
        )
        ''')


def put_to_db(cursor, url, title, price_usd, odometer, username, phone_number, images_urls, images_count,
              car_number, car_vin, datetime_found):
    cursor.execute('''
        INSERT INTO cars (url, title, price_usd, odometer, username, phone_number, images_urls, images_count, car_number,
    car_vin, datetime_found)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
        url, title, price_usd, odometer, username, phone_number, ','.join(images_urls), images_count,
        car_number,
        car_vin, datetime_found))


def is_url_in_db(url: str, cursor) -> bool:
    cursor.execute('SELECT url FROM cars WHERE url = %s', (url,))
    return bool(cursor.fetchone())


def create_database_dump():
    dump_folder = 'dumps'
    if not os.path.exists(dump_folder):
        os.makedirs(dump_folder)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_filename = f"{dump_folder}/dump_{timestamp}.sql"

    os.environ['PGPASSWORD'] = 'postgres'

    pg_dump_command = [
        'pg_dump',
        '-U', 'postgres',
        '-h', 'localhost',
        '-p', '5433',
        'autoria_info',
    ]

    try:
        with open(dump_filename, 'w') as dump_file:
            subprocess.run(pg_dump_command, stdout=dump_file, stderr=subprocess.PIPE, text=True, check=True)
        print(f"Database dumped to {dump_filename}")
    except subprocess.CalledProcessError as e:
        print(f"Error while creating database dump: {e}")
        print(e.stderr)
    finally:
        os.environ.pop('PGPASSWORD', None)
