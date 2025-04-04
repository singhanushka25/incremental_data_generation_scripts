import re

import pandas as pd

# from config_manager.read_config import config_handler

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from config_manager.read_config import config_handler

config_path = os.path.join(project_dir, "config_manager", "config.txt")
config = config_handler(config_path)



def extract_data_types(sql_statements):
    data_types = set()

    for statement in sql_statements:
        match = re.search(r"\((.*?)\);", statement)
        if match:
            column_definitions = match.group(1)  # Get column definitions inside parentheses
            columns = column_definitions.split(",")  # Split by comma to get individual columns
            for column in columns:
                parts = column.strip().split()  # Split by spaces
                if len(parts) > 1:
                    data_types.add(parts[1].upper())  # Take the second element as the data type

    return sorted(data_types)


# config = config_handler("config.txt")
file_path = config["csv_path"]
df = pd.read_csv(file_path, header=None)
sql_statements = df[0].tolist()
extracted_data_types = extract_data_types(sql_statements)


# [
#  'ARRAY',
#  'BIGINT',
#  'BOOLEAN',
#  'BYTEA',
#  'CHARACTER VARYING',
#  'DATE',
#  'DOUBLE',
#  'HSTORE',
#  'INTEGER',
#  'JSON',
#  'JSONB',
#  'NUMERIC,
#  'OID',
#  'REAL',
#  'SMALLINT',
#  'TIME',
#  'TIMESTAMP',
#  'USER_DEFINED',
#  'UUID']
