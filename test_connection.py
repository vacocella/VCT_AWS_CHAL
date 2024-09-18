import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Get the DATABASE_URL from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set in environment variables.")
        sys.exit(1)

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(DATABASE_URL)
        print("Connection to PostgreSQL DB successful.")

        # Optionally, execute a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("PostgreSQL database version:", record[0])

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:")
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()
