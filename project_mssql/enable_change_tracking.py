import re

import pandas as pd
import pymssql
from faker import Faker

# from customer_schema_data_generation_scripts.config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)


fake = Faker()


def parse_create_table(sql):
    # Extract the table name
    table_match = re.search(r'CREATE TABLE "?(\w+)"?', sql, re.IGNORECASE)
    if not table_match:
        raise ValueError("Table name not found in SQL statement.")
    table_name = table_match.group(1)

    # Extract column definitions correctly
    column_pattern = re.findall(r'"(\w+)"\s+([\w]+(?:\(\d+(?:,\d+)?\))?)', sql)
    if not column_pattern:
        raise ValueError("No valid columns found in SQL statement.")

    return table_name, column_pattern


def main() -> None:
    try:
        conn = pymssql.connect(
            server="database-3.ciegf1bj3i1y.us-east-1.rds.amazonaws.com",
            user="admin",
            password="***",
            database="tan_data",
        )
        cursor = conn.cursor()
        # config = config_handler("config.txt")
        df = pd.read_csv(config["csv_path"], header=None)
        sql_statements = df[0].tolist()[150:200]

        for create_table_sql in sql_statements:
            create_table_sql = re.sub(r"\`\w+\`\.", "", create_table_sql)
            create_table_sql = re.sub(r"ALTER TABLE .*?;", "", create_table_sql, flags=re.S)
            create_table_sql = create_table_sql.strip().replace("`", '"')

            try:
                table_name, column_list = parse_create_table(create_table_sql)

                if len(column_list) >= 2:
                    pk_columns = [column_list[0], column_list[2]]  # Each element is (column_name, data_type)

                    # Alter columns to be NOT NULL
                    for col_name, col_type in pk_columns:
                        alter_sql = f"ALTER TABLE dbo.{table_name} ALTER COLUMN {col_name} {col_type} NOT NULL"
                        cursor.execute(alter_sql)
                        conn.commit()

                    # Define Primary Key
                    pk_sql = f"ALTER TABLE dbo.{table_name} ADD CONSTRAINT PK_{table_name} PRIMARY KEY ({', '.join([col[0] for col in pk_columns])})"
                    cursor.execute(pk_sql)
                    conn.commit()
                else:
                    pass

                # Enable change tracking
                cursor.execute(f"ALTER TABLE dbo.{table_name} ENABLE CHANGE_TRACKING")
                conn.commit()

            except Exception:
                pass

        conn.close()

    except Exception:
        pass


if __name__ == "__main__":
    main()
