import sqlite3
import os

DB_NAME = "crm_enterprise.db"

def check_schema():
    if not os.path.exists(DB_NAME):
        print(f"Database {DB_NAME} not found!")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    tables = ["quotations", "follow_ups", "contracts", "payment_plans"]
    for table in tables:
        print(f"\nSchema for table: {table}")
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        for c in cols:
            print(f"  Column: {c[1]} ({c[2]})")
    
    conn.close()

if __name__ == "__main__":
    check_schema()
