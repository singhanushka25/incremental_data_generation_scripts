import psycopg2
from datetime import datetime
import sys
import os

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)



def get_tables_with_timestamp(cursor):
    """Get all tables that have an UPDATED_TIMESTAMP column"""
    cursor.execute("""
        SELECT DISTINCT table_name 
        FROM information_schema.columns 
        WHERE column_name = 'timestamp'
        AND table_schema = 'public'
    """)
    return [table[0] for table in cursor.fetchall()]


def check_id_column_exists(cursor, table_name):
    """Check if 'id' column exists in the given table"""
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name = 'id'
            AND table_schema = 'public'
        )
    """, (table_name,))
    return cursor.fetchone()[0]


def add_primary_key(cursor, table_name):
    """Add primary key constraint to id column if it exists"""
    try:
        if check_id_column_exists(cursor, table_name):
            cursor.execute(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY (id)')
            # print(f"Added primary key constraint to {table_name}")
        else:
            # print(f"Table {table_name} does not have an id column")
            pass  # Add this line
    except psycopg2.Error as e:
        if "already has a primary key" in str(e):
            # print(f"Table {table_name} already has a primary key")
            pass
        else:
            # print(f"Error adding primary key to {table_name}: {str(e)}")
            pass




def update_random_records(cursor, table_name):
    """Update UPDATED_TIMESTAMP for 100 random records in the given table"""
    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        total_records = cursor.fetchone()[0]

        if total_records == 0:
            # print(f"No records found in table {table_name}")
            return

        records_to_update = min(100, total_records)

        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')

        # PostgreSQL syntax for updating random records
        update_query = f"""
            UPDATE "{table_name}"
            SET timestamp = %s
            WHERE id IN (
                SELECT id
                FROM "{table_name}"
                ORDER BY RANDOM()
                LIMIT {records_to_update}
            )
        """

        cursor.execute(update_query, (current_timestamp,))
        print(f"Updated {table_name}")

    except Exception as e:
        # print(f"Error updating table {table_name}: {str(e)}")
        pass

def main():
    # config = config_handler("config_manager/config.txt")

    try:
        conn = psycopg2.connect(
            dbname=config["database"],
            user=config["user"],
            password=config["password"],
            host=config["server"]
        )
        cursor = conn.cursor()

        tables = get_tables_with_timestamp(cursor)

        if not tables:
            # print("No tables found with UPDATED_TIMESTAMP column")
            return

        for table in tables:
            add_primary_key(cursor, table)
            conn.commit()
            update_random_records(cursor, table)
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
