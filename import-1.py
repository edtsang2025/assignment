import csv
import psycopg2
import os

from dotenv import load_dotenv
load_dotenv()

db_password = os.getenv('DB_PASSWORD')

# Database connection setup
conn = psycopg2.connect(
    dbname="exercise",
    user="postgres",
    password=db_password,
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Create table (if not exists)
cur.execute("""
CREATE TABLE IF NOT EXISTS exercise_table_1 (
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT
)
""")
conn.commit()

# Clear existing data
cur.execute("DELETE FROM exercise_table_1")

# Counter for successful imports
imported_count = 0

# Read and validate CSV
with open('exercise-data-1.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        phone = row['Phone Number']
        if phone.isdigit() and len(phone) == 8:
            cur.execute("""
                INSERT INTO exercise_table_1 (username, first_name, last_name, phone_number)
                VALUES (%s, %s, %s, %s)
            """, (row['Username'], row['First Name'], row['Last Name'], phone))
            imported_count += 1
        else:
            print(f"Invalid phone number for {row['Username']}: {phone}")

conn.commit()
cur.close()
conn.close()

# Print summary
print(f"Imported {imported_count} valid records into the database.")