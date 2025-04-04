import csv
import psycopg2
import sys
import os
import traceback  # Import traceback for detailed error logging

# Get project directory
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

# Load configuration
config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)

# Debug: Print loaded config values
print("Loaded configuration:", config)

try:
    # Connect to the database
    print("Connecting to the database...")
    conn = psycopg2.connect(
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
        host=config["server"],
    )
    print("Database connection successful.")
except Exception as e:
    print("Error connecting to database:", str(e))
    sys.exit(1)  # Exit if connection fails

cur = conn.cursor()

# Open the CSV file
try:
    print(f"Opening CSV file: {config['csv_path']}")
    with open(config["csv_path"]) as file:
        reader = csv.reader(file)
        # next(reader)  # Skip header row
        counter = 0

        for row in reader:
            counter += 1
            value = row[0].split(";")[0]

            # Debug: Print current row data
            print(f"Processing row {counter}: {value}")

            try:
                cur.execute(value)  # Execute the raw SQL from CSV
                cur.execute("CALL insert_test_data(%s, %s)", (value, 50))  # Execute stored procedure
                conn.commit()
                print(f"Row {counter} executed successfully.")

            except Exception as e:
                conn.rollback()  # Rollback if error occurs
                print(f"Error processing row {counter}: {value}")
                print(traceback.format_exc())  # Print full error traceback

    conn.commit()  # Final commit after processing all rows
    print("All rows processed successfully.")

except FileNotFoundError:
    print(f"Error: CSV file not found at {config['csv_path']}")
except Exception as e:
    print("Unexpected error while reading CSV:", str(e))
    print(traceback.format_exc())

# Close connections
finally:
    cur.close()
    conn.close()
    print("Database connection closed.")
