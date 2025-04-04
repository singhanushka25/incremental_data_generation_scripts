import re

import pandas as pd

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)

def extract_datatypes(create_table_sql):
    match = re.search(r"\((.*)\)", create_table_sql, re.DOTALL)
    if not match:
        return "No table definition found."

    table_content = match.group(1)

    lines = [line.strip() for line in table_content.split(",\n")]

    column_types = {}

    for line in lines:
        parts = line.split()
        if parts[0].upper() in ["PRIMARY", "UNIQUE", "KEY"]:
            continue
        column_name = parts[0].strip("`")
        column_type = parts[1]
        column_types[column_name] = column_type
    return set(column_types.values())


def extract_data_types(sql_statements):
    final_datatypes = set()
    count = 0
    for statement in sql_statements:
        count += 1
        final_datatypes.update(extract_datatypes(statement))
    return sorted(final_datatypes)


# config = config_handler("config.txt")
file_path = config["csv_path"]
df = pd.read_csv(file_path, header=None)
sql_statements = df[0].tolist()
extracted_data_types = extract_data_types(sql_statements)


# 'bigint', 'bit(1)', 'char(1)', 'date', 'datetime', 'datetime(6)', 'decimal(16,12)', 'decimal(16,2)', 'decimal(16,4)', 'decimal(21,9)', 'double', 'float(6,2)', 'int', 'json', 'longblob', 'longtext', 'mediumblob', 'mediumtext', 'smallint', 'text', 'timestamp', 'timestamp(6)', 'tinyblob', 'tinyint', 'tinyint(1)', 'varchar(10)', 'varchar(100)', 'varchar(1000)', 'varchar(1004)', 'varchar(140)', 'varchar(15)', 'varchar(1500)', 'varchar(20)', 'varchar(200)', 'varchar(250)', 'varchar(2500)', 'varchar(255)', 'varchar(256)', 'varchar(30)', 'varchar(32)', 'varchar(35)', 'varchar(36)', 'varchar(4)', 'varchar(4000)', 'varchar(4096)', 'varchar(45)', 'varchar(50)', 'varchar(500)', 'varchar(6)', 'varchar(60)', 'varchar(600)', 'varchar(64)', 'varchar(70)', 'varchar(72)', 'varchar(750)']
