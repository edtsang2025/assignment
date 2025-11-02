import csv
import re
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
CREATE TABLE IF NOT EXISTS exercise_table_3 (
    job_title TEXT,
    department TEXT,
    last_name TEXT,
    first_name TEXT,
    email TEXT,
    phone_number TEXT
)
""")
conn.commit()

# Clear existing data
cur.execute("DELETE FROM exercise_table_3")

# Email validation regex
email_pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

# Counter for successful imports
imported_count = 0

# Read and validate CSV
with open('exercise-data-3.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        phone = row['Phone Number']
        email = row['Email']
        if phone.isdigit() and len(phone) == 8 and email_pattern.match(email):
            cur.execute("""
                INSERT INTO exercise_table_3 (job_title, department, last_name, first_name, email, phone_number)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['Job Title'],
                row['Department'],
                row['Last Name'],
                row['First Name'],
                email,
                phone
            ))
            imported_count += 1
        else:
            print(f"Invalid record for {row['First Name']} {row['Last Name']}: Phone={phone}, Email={email}")

conn.commit()
cur.close()
conn.close()

# Print summary
print(f"Imported {imported_count} valid employees records into the database.")
