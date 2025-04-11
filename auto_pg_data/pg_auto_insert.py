import psycopg2
import time
from datetime import datetime

# Configuration
POSTGRES_DB = "postgres"
NEW_DB = "newdb" 
DB_USER = "postgres"
DB_PASSWORD = "new_password"
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAMES = [f"table_{i+1}" for i in range(5)]

def create_database():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (NEW_DB,))
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {NEW_DB}")
            print(f" Database '{NEW_DB}' created.")
        else:
            print(f" Database '{NEW_DB}' already exists.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f" Error creating database: {e}")

def create_tables():
    try:
        conn = psycopg2.connect(
            dbname=NEW_DB,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for table in TABLE_NAMES:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    created_at TIMESTAMP
                );
            """)
            print(f"  Table '{table}' is ready.")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f" Error creating tables: {e}")

def insert_data():
    try:
        conn = psycopg2.connect(
            dbname=NEW_DB,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for table in TABLE_NAMES:
            now = datetime.now()
            message = f"Data at {now.strftime('%Y-%m-%d %H:%M:%S')}"
            cursor.execute(
                f"INSERT INTO {table} (message, created_at) VALUES (%s, %s)",
                (message, now)
            )
            print(f" Inserted into '{table}' at {now}.")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f" Error inserting data: {e}")

def main():
    create_database()
    create_tables()

    while True:
        insert_data()
        time.sleep(10)  # 5 minutes

if __name__ == "__main__":
    main()
