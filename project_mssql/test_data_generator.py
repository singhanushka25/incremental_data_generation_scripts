import random
import re
import string

import numpy as np
import pandas as pd
import pymssql
from faker import Faker
import sys
import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# from customer_schema_data_generation_scripts.config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)

fake = Faker()


def insert_bulk_data(cursor, table_name, columns, num_rows=200, batch_size=200) -> None:
    column_names = ", ".join([f"[{col[0]}]" for col in columns])
    placeholders = ", ".join(["%s" for _ in columns])
    insert_query = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"

    bulk_data = [tuple(generate_random_value(col[1]) for col in columns) for _ in range(num_rows)]
    for i in range(0, num_rows, batch_size):
        cursor.executemany(insert_query, bulk_data[i : i + batch_size])


def generate_random_value(data_type):
    if data_type == "bigint":
        return int(np.random.randint(-9223372036854775808, 9223372036854775807))
    if data_type == "int":
        return int(np.random.randint(-2147483648, 2147483647))
    if data_type == "smallint":
        return int(np.random.randint(-32768, 32767))
    if data_type == "tinyint":
        return int(np.random.randint(0, 255))
    if data_type == "bit":
        return bool(np.random.choice([0, 1]))
    if "decimal" in data_type or "numeric" in data_type:
        return float(np.round(np.random.uniform(1.0, 1000.0), 2))  # Faster float generation
    if data_type in ("float", "real"):
        return float(np.random.uniform(1.0, 1000.0))
    if data_type in ("money"):
        return float(np.round(np.random.uniform(1.0, 100000.0), 2))
    if data_type.startswith(("varchar", "nvarchar")):
        length = int(re.findall(r"\d+", data_type)[0])
        return "".join(np.random.choice(list(string.ascii_letters + string.digits), size=length))
    if re.match(r"char\((\d+)\)", data_type) or re.match(r"varchar\((\d+)\)", data_type):
        length = int(re.findall(r"\d+", data_type)[0])
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))
    if re.match(r"nchar\((\d+)\)", data_type) or re.match(r"nvarchar\((\d+)\)", data_type):
        length = int(re.findall(r"\d+", data_type)[0])
        return fake.text(max_nb_chars=length)
    if data_type in ("date", "smalldatetime", "datetime", "datetime2", "datetimeoffset"):
        return fake.date_time_between(start_date="-10y", end_date="now")
    if data_type == "uniqueidentifier":
        return str(fake.uuid4())
    if data_type == "xml":
        return f"<note><to>{fake.first_name()}</to><from>{fake.last_name()}</from></note>"
    return None


def parse_create_table(sql):
    # Extract the table name
    table_match = re.search(r"CREATE TABLE \"?(\w+)\"?", sql, re.IGNORECASE)
    if not table_match:
        raise ValueError("Table name not found in SQL statement.")
    table_name = table_match.group(1)

    # Extract column definitions correctly
    column_pattern = re.findall(r'"(\w+)"\s+([\w]+(?:\(\d+(?:,\d+)?\))?)', sql)
    if not column_pattern:
        raise ValueError("No valid columns found in SQL statement.")

    return table_name, column_pattern


def main() -> None:
    # config = config_handler("config_manager/config.txt")
    try:
        conn = pymssql.connect(
            server=config["server"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        cursor = conn.cursor()
        df = pd.read_csv(config["csv_path"], header=None)
        sql_statements = df[0].tolist()
        for create_table_sql in sql_statements:
            create_table_sql = re.sub(r"\`\w+\`\.", "", create_table_sql)
            create_table_sql = re.sub(r"ALTER TABLE .*?;", "", create_table_sql, flags=re.S)
            create_table_sql = create_table_sql.strip().replace("`", '"')
            table_name, column_list = parse_create_table(create_table_sql)
            try:
                cursor.execute(create_table_sql)
                insert_bulk_data(cursor, table_name, column_list, num_rows=200)
                conn.commit()

            except Exception:
                pass
        conn.close()

    except Exception:
        pass


if __name__ == "__main__":
    main()
