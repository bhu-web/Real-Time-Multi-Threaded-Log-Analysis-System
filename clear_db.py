import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(__file__), "output", "analysis_results.db")

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Delete all rows from the table
cursor.execute("DELETE FROM alerts;")

# Optional: Reset the auto-incrementing ID counter back to 1
cursor.execute("DELETE FROM sqlite_sequence WHERE name='alerts';")

conn.commit()
conn.close()
print("[System] SQLite table 'alerts' has been completely cleared!")