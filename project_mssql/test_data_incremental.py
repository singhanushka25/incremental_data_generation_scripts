import random
import sys
import os
from datetime import datetime

import pymssql

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# from customer_schema_data_generation_scripts.config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)


def get_tables_with_id_columns(cursor):
    cursor.execute("""
        WITH IdColumns AS (
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                CASE
                    WHEN LOWER(COLUMN_NAME) IN ('id', 'sourceid') THEN 1
                    ELSE 2
                END as Priority,
                ROW_NUMBER() OVER (PARTITION BY TABLE_NAME 
                    ORDER BY 
                        CASE 
                            WHEN LOWER(COLUMN_NAME) IN ('id', 'sourceid') THEN 1
                            ELSE 2
                        END,
                        COLUMN_NAME
                ) as RN
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'dbo'
            AND (COLUMN_NAME LIKE '%id' OR COLUMN_NAME LIKE '%ID')
        )
        SELECT TABLE_NAME, COLUMN_NAME
        FROM IdColumns
        WHERE RN = 1
    """)
    return {table: column for table, column in cursor.fetchall()}


def update_random_records(cursor, table_name, column_name):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        total_records = cursor.fetchone()[0]
        
        if total_records == 0:
            print(f"No records found in table {table_name}")
            return
        
        # Determine which date column to update
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}' 
            AND COLUMN_NAME IN ('DateChanged', 'LastUpdatedDate','lastUpdatedDate','UpdateDateTime','last_updated','updated_date')
        """)
        date_column = cursor.fetchone()
        
        if not date_column:
#             print(f"No date column found in table {table_name}")
            return
        
        date_column_name = date_column[0]
        records_to_update = min(100, total_records)

        current_datechanged = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        update_query = f"""
            UPDATE t
            SET {date_column_name} = %s
            FROM [{table_name}] t
            INNER JOIN (
                SELECT TOP {records_to_update} {column_name}
                FROM [{table_name}]
                ORDER BY NEWID()
            ) r ON t.{column_name} = r.{column_name}
        """
        
        cursor.execute(update_query, (current_datechanged,))
#         print(f"Updated {table_name}: {column_name} with {date_column_name}")

    except Exception as e:
        print(f"Error updating table {table_name}: {str(e)}")


def main():
    # config = config_handler("config_manager/config.txt")
    
    try:
        conn = pymssql.connect(
            server=config["server"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        cursor = conn.cursor()

        # Get tables and their primary ID columns
        tables_with_ids = get_tables_with_id_columns(cursor)
        
        # Update each table using its identified ID column
        for table, id_column in tables_with_ids.items():
            update_random_records(cursor, table, id_column)
            conn.commit()
        
    except Exception as e:
        print(f"Database error: {str(e)}")
    
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main() 