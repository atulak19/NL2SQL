import sqlite3
import os

DB_FILE = "nl2sql.db"

# Initialize database with sample employee data
def setup_database():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed {DB_FILE} successfully !")
    
    conn = None
    
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        print(f"Connected to database {DB_FILE}")
        
        # Create employees table with basic fields
        cursor.execute(
            '''
            CREATE TABLE employees(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                department TEXT NOT NULL,
                salary INTEGER NOT NULL,
                city TEXT NOT NULL
            )
            ''' 
        )
        print("Table 'employees' created successfully !")
        
        # Add some test data
        sample_employees = [
            ('Alice Johnson', 'Sales', 70000, 'Pune'),
            ('Bob Williams', 'IT', 85000, 'Mumbai'),
            ('Charlie Brown', 'HR', 60000, 'Pune'),
            ('Diana Miller', 'Sales', 72000, 'Bangalore'),
            ('Eve Davis', 'IT', 90000, 'Mumbai'),
            ('Frank White', 'Marketing', 65000, 'Pune'),
            ('Grace Green', 'IT', 92000, 'Bangalore'),
            ('Harry Black', 'Sales', 75000, 'Mumbai'),
            ('Ivy Blue', 'HR', 62000, 'Bangalore'),
            ('Jack Red', 'Marketing', 68000, 'Mumbai')
        ]
        cursor.executemany('INSERT INTO employees (name,department,salary,city) VALUES(?,?,?,?)', sample_employees)
        conn.commit()
        print(f"Inserted {len(sample_employees)} in 'employees' table")
        print("DB setup completed")
        
    except sqlite3.error as e:
        print(f"Database error: {e}")
    except exception as e:
        print(f"An unexpected error is occured: {e}")
    finally:
        if conn:
            conn.close()
            print("DB connection closed.")

if __name__ == "__main__":
    setup_database()
    

