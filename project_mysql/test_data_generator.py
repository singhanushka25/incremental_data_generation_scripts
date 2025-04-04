import datetime
import json
import random
import re
import string
from typing import Any
import sys
import os

import mysql.connector
import pandas as pd
from mysql.connector import Error

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# from customer_schema_data_generation_scripts.config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)


class MockDataGenerator:
    def __init__(self, create_table_statement: str):
        self.create_table_statement = create_table_statement
        self.table_name = self._extract_table_name()
        self.columns = self._parse_columns()

    def _extract_table_name(self) -> str:
        """Extract table name from CREATE TABLE statement."""
        match = re.search(r"CREATE TABLE\s+`?(\w+)`?", self.create_table_statement, re.IGNORECASE)
        if not match:
            raise ValueError("Could not find table name in CREATE TABLE statement")
        return match.group(1)

    def _parse_columns(self) -> list[dict[str, Any]]:
        """Parse column definitions from CREATE TABLE statement."""
        clean_stmt = re.sub(r"/\*.*?\*/", "", self.create_table_statement)
        clean_stmt = re.sub(r"--.*$", "", clean_stmt, flags=re.MULTILINE)

        columns_match = re.search(r"CREATE TABLE.*?\((.*)\)", clean_stmt, re.DOTALL | re.IGNORECASE)
        if not columns_match:
            raise ValueError("Could not parse column definitions")

        columns_text = columns_match.group(1)
        column_defs = []

        level = 0
        current = []
        for char in columns_text:
            if char == "(":
                level += 1
            elif char == ")":
                level -= 1
            elif char == "," and level == 0:
                column_defs.append("".join(current).strip())
                current = []
                continue
            current.append(char)
        if current:
            column_defs.append("".join(current).strip())

        columns = []
        for col_def in column_defs:
            if any(keyword in col_def.upper() for keyword in ["CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE"]):
                continue

            # Enhanced regex to capture full data type including precision/scale
            match = re.match(r"`?(\w+)`?\s+([^,\n]+)", col_def.strip())
            if match:
                name, full_type = match.groups()
                # Extract base type and parameters
                type_match = re.match(r"(\w+)(?:\((.*?)\))?", full_type.strip().split()[0])
                if type_match:
                    base_type = type_match.group(1).upper()
                    params = type_match.group(2) if type_match.group(2) else None

                    columns.append(
                        {"name": name, "type": base_type, "params": params, "full_type": full_type.strip().upper()}
                    )

        return columns

    def _safe_float(self, min_val, max_val, decimals=2):
        """Generate a safe float value within bounds."""
        try:
            value = random.uniform(min_val, max_val)
            if not float("-inf") < value < float("inf"):
                return 0.0
            return round(value, decimals)
        except (OverflowError, ValueError):
            return 0.0

    def _generate_mock_value(self, column: dict[str, Any]) -> Any:
        """Generate a mock value based on column type and parameters."""
        base_type = column["type"].upper()
        full_type = column["full_type"]
        params = column["params"]

        try:
            # Handle BIGINT
            if base_type == "BIGINT":
                return random.randint(-9223372036854775808, 9223372036854775807)

            # Handle BIT(1)
            if "BIT(1)" in full_type:
                return random.choice([0, 1])

            # Handle various CHAR types
            if base_type == "CHAR":
                length = int(params) if params else 1
                return "".join(random.choices(string.ascii_letters, k=length))

            # Handle DATE
            if base_type == "DATE":
                start_date = datetime.date(2000, 1, 1)
                end_date = datetime.date(2023, 12, 31)
                days_between = (end_date - start_date).days
                random_days = random.randint(0, days_between)
                return start_date + datetime.timedelta(days=random_days)

            # Handle DATETIME with precision
            if base_type == "DATETIME":
                start_date = datetime.datetime(2000, 1, 1)
                end_date = datetime.datetime(2023, 12, 31)
                time_between = end_date - start_date
                days_between = time_between.days
                random_days = random.randint(0, days_between)
                random_seconds = random.randint(0, 86400)
                dt = start_date + datetime.timedelta(days=random_days, seconds=random_seconds)
                precision = int(params) if params else 0
                return dt.replace(microsecond=random.randint(0, 999999) if precision > 0 else 0)

            # Handle various DECIMAL types
            if base_type == "DECIMAL":
                if params:
                    precision, scale = map(int, params.split(","))
                    max_val = min(10 ** (precision - scale) - 1, 1e9)  # More conservative limit
                    min_val = -max_val if not any(s in full_type for s in ["UNSIGNED", "ZEROFILL"]) else 0
                    return self._safe_float(min_val, max_val, scale)
                return self._safe_float(-1000000, 1000000, 2)

            # Handle DOUBLE - with very conservative limits
            if base_type == "DOUBLE":
                return self._safe_float(-1e9, 1e9, 6)

            # Handle FLOAT with precision
            if base_type == "FLOAT":
                if params:
                    precision, scale = map(int, params.split(","))
                    max_val = min(10 ** (precision - scale), 1e6)  # Very conservative limit
                    min_val = -max_val
                    return self._safe_float(min_val, max_val, scale)
                return self._safe_float(-1e6, 1e6, 2)

            # Handle INT
            if base_type == "INT":
                return random.randint(-2147483648, 2147483647)

            # Handle JSON
            if base_type == "JSON":
                return json.dumps(
                    {
                        "id": random.randint(1, 1000),
                        "name": "".join(random.choices(string.ascii_letters, k=8)),
                        "value": self._safe_float(0, 100, 2),
                    }
                )

            # Handle LONGBLOB and MEDIUMBLOB
            if base_type in ["LONGBLOB", "MEDIUMBLOB"]:
                return bytes(random.getrandbits(8) for _ in range(64))

            # Handle LONGTEXT, MEDIUMTEXT, and TEXT
            if base_type in ["LONGTEXT", "MEDIUMTEXT", "TEXT"]:
                length = random.randint(50, 200)
                return " ".join(
                    "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(3, 8)))
                    for _ in range(length)
                )

            # Handle SMALLINT
            if base_type == "SMALLINT":
                return random.randint(-32768, 32767)

            # Handle TIMESTAMP with precision
            if base_type == "TIMESTAMP":
                dt = datetime.datetime.fromtimestamp(random.randint(0, 2147483647))
                precision = int(params) if params else 0
                return dt.replace(microsecond=random.randint(0, 999999) if precision > 0 else 0)

            # Handle TINYINT(1) and TINYINT
            if base_type == "TINYINT":
                if params == "1":  # TINYINT(1) is often used as BOOLEAN
                    return random.choice([0, 1])
                return random.randint(-128, 127)

            # Handle VARCHAR with specific lengths
            if base_type == "VARCHAR":
                max_length = int(params) if params else 255
                length = random.randint(1, min(max_length, 50))
                return "".join(random.choices(string.ascii_letters + string.digits + " ", k=length))

            return None

        except Exception:
            # Return safe default values based on type
            if base_type in ["DOUBLE", "FLOAT", "DECIMAL"]:
                return 0.0
            if base_type in ["INT", "BIGINT", "SMALLINT", "TINYINT"]:
                return 0
            if base_type in ["VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT"]:
                return ""
            if base_type == "JSON":
                return "{}"
            if base_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                return datetime.datetime.now()
            if base_type in ["LONGBLOB", "MEDIUMBLOB"]:
                return b""
            return None

    def generate_mock_data(self, num_records: int) -> list[dict[str, Any]]:
        """Generate specified number of mock records."""
        mock_data = []
        for _ in range(num_records):
            record = {}
            for column in self.columns:
                record[column["name"]] = self._generate_mock_value(column)
            mock_data.append(record)
        return mock_data

    def insert_data(self, mock_data: list[dict[str, Any]], db_config: dict[str, str]) -> None:
        """Insert mock data into the database."""
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            columns = ", ".join([f"`{col['name']}`" for col in self.columns])
            placeholders = ", ".join(["%s"] * len(self.columns))
            insert_query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders})"

            values = []
            for record in mock_data:
                row = tuple(record[col["name"]] for col in self.columns)
                values.append(row)

            # Insert in batches of 3000
            batch_size = 500
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()

        except Error:
            pass

        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()


# Example usage
if __name__ == "__main__":
    # config = config_handler("config_manager/config.txt")
    db_config = {
        "host": config["server"],
        "user": config["user"],
        "password": config["password"],
        "database": config["database"],
    }
    file_path = config["csv_path"]
    df = pd.read_csv(file_path, header=None)
    sql_statements = df[0].tolist()[127:]
    conn = mysql.connector.connect(**db_config)
    for create_table_stmt in sql_statements:
        try:
            cursor = conn.cursor()
            # Execute the CREATE TABLE statement
            cursor.execute(create_table_stmt)
            conn.commit()
            # Close cursor and connection
            cursor.close()

        except Error:
            pass
        # Initialize generator
        generator = MockDataGenerator(create_table_stmt)

        # Generate 1000 mock records
        mock_data = generator.generate_mock_data(500)

        # Insert data into database
        generator.insert_data(mock_data, db_config)
    conn.close()
