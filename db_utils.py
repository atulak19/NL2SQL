from nt import error
from pydoc import describe
import sqlite3

DB_FILE = "nl2sql.db"

# Setup database connection with Row factory for dict-like access
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# Run a query and return results with column names
def fetch_data(query, params=()):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query,params)
        results = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        return results, columns, None
    except sqlite3.Error as e:
        print(f"Database query error: {e}")
        return [], [], f"Database error: {e}"
    finally:
        conn.close()

# Get column names from employees table
def get_table_schema():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(employees);")
        schema_info = cursor.fetchall()
        columns = [col['name'] for col in schema_info]
        return columns
    except sqlite3.Error as e:
        print(f"Error getting table schema: {e}")
        return []
    finally:
        conn.close()
    