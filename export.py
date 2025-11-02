import csv
import psycopg2
import os
import re
from psycopg2 import sql


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

# Fetch all table names from public schema
cur.execute("""
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'public'
""")
tables = cur.fetchall()

# Display tables with numbering
print("\nAvailable tables:")
for idx, table in enumerate(tables, start=1):
    print(f"{idx}. {table[0]}")

# Prompt user to select a table by number
try:
    choice = int(input("\nEnter the number of the table to export: "))
    if choice < 1 or choice > len(tables):
        raise ValueError("Invalid table number.")
    table_name = tables[choice - 1][0]
except ValueError as ve:
    print(f"Error: {ve}")
    cur.close()
    conn.close()
    exit()

# Sanitize filename
safe_name = re.sub(r'[^\w\-]', '_', table_name)
filename = f"{safe_name}_export.csv"

# Export selected table to CSV
try:
    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
    cur.execute(query)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    with open(filename, mode="w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(column_names)
        writer.writerows(rows)

    print(f"\n✅ Exported {len(rows)} records to '{filename}'")

except Exception as e:
    print(f"\n❌ Error exporting table: {e}")

finally:
    cur.close()
    conn.close()
