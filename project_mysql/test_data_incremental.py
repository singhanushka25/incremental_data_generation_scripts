import mysql.connector
from mysql.connector import Error
import sys
import os
from datetime import datetime
import random

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# from customer_schema_data_generation_scripts.config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)


def get_tables_with_timestamp(cursor):
    """Get all tables that have an UPDATED_TIMESTAMP column"""
    cursor.execute("""
        SELECT DISTINCT TABLE_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE COLUMN_NAME = 'UPDATED_TIMESTAMP' 
        AND TABLE_SCHEMA = DATABASE()
    """)
    return [table[0] for table in cursor.fetchall()]

def update_random_records(cursor, table_name):
    """Update UPDATED_TIMESTAMP for 100 random records in the given table"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        total_records = cursor.fetchone()[0]
        
        if total_records == 0:
            print(f"No records found in table {table_name}")
            return
        
        records_to_update = min(100, total_records)

        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        update_query = f"""
            UPDATE `{table_name}` 
            SET UPDATED_TIMESTAMP = %s
            WHERE PK IN (
                SELECT PK
                FROM (
                    SELECT PK
                    FROM `{table_name}` 
                    ORDER BY RAND() 
                    LIMIT {records_to_update}
                ) tmp
            )
        """
        
        cursor.execute(update_query, (current_timestamp,))
#         print(f"Updated {records_to_update} records in {table_name}")
        
    except Error as e:
        print(f"Error updating table {table_name}: {str(e)}")

def main():
    # config = config_handler("config.txt")
    db_config = {
        "host": config["server"],
        "user": config["user"],
        "password": config["password"],
        "database": config["database"],
    }
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        tables = get_tables_with_timestamp(cursor)
        
        if not tables:
            print("No tables found with UPDATED_TIMESTAMP column")
            return
        
        for table in tables:
            update_random_records(cursor, table)
            conn.commit()
        
    except Error as e:
        print(f"Database error: {str(e)}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 