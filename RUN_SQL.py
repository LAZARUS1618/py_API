# Ethan Gueck
# This code is intended to be a quick reference for running SQL scripts.

import psycopg2
conn = psycopg2.connect(
    host="REDACTED",
    port="REDACTED",
    database="REDACTED",
    user="REDACTED",
    password="REDACTED"
)

cur = conn.cursor()
sql_script = """
REDACTED
"""
cur.execute(sql_script)
results = cur.fetchall()
for row in results:
    print(row)
cur.close()
conn.close()