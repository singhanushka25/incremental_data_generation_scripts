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


# ['bigint', 'bit', 'char(1)', 'char(10)', 'char(13)', 'char(2)', 'char(3)', 'char(30)', 'char(32)', 'char(7)', 'char(8)', 'date', 'datetime', 'datetime2', 'datetimeoffset', 'decimal(10,', 'decimal(11,', 'decimal(12,', 'decimal(13,', 'decimal(15,', 'decimal(18,', 'decimal(19,', 'decimal(3,', 'decimal(4,', 'decimal(5,', 'decimal(6,', 'decimal(7,', 'decimal(8,', 'decimal(9,', 'float', 'in', 'int', 'money', 'nchar(1)', 'nchar(10)', 'nchar(11)', 'nchar(15)', 'nchar(18)', 'nchar(2)', 'nchar(20)', 'nchar(30)', 'nchar(4)', 'nchar(5)', 'nchar(6)', 'numeric(14,', 'numeric(15,', 'numeric(16,', 'numeric(17,', 'numeric(18,', 'numeric(19,', 'numeric(20,', 'numeric(5,', 'numeric(8,', 'numeric(9,', 'nvarchar(1)', 'nvarchar(10)', 'nvarchar(100)', 'nvarchar(1000)', 'nvarchar(1024)', 'nvarchar(12)', 'nvarchar(127)', 'nvarchar(128)', 'nvarchar(15)', 'nvarchar(150)', 'nvarchar(16)', 'nvarchar(18)', 'nvarchar(2)', 'nvarchar(20)', 'nvarchar(200)', 'nvarchar(2000)', 'nvarchar(2048)', 'nvarchar(24)', 'nvarchar(25)', 'nvarchar(250)', 'nvarchar(254)', 'nvarchar(255)', 'nvarchar(256)', 'nvarchar(260)', 'nvarchar(3)', 'nvarchar(30)', 'nvarchar(300)', 'nvarchar(3000)', 'nvarchar(32)', 'nvarchar(35)', 'nvarchar(4)', 'nvarchar(40)', 'nvarchar(400)', 'nvarchar(4000)', 'nvarchar(454)', 'nvarchar(469)', 'nvarchar(48)', 'nvarchar(5)', 'nvarchar(50)', 'nvarchar(500)', 'nvarchar(512)', 'nvarchar(6)', 'nvarchar(60)', 'nvarchar(600)', 'nvarchar(63)', 'nvarchar(64)', 'nvarchar(70)', 'nvarchar(8)', 'nvarchar(80)', 'nvarchar(9)', 'nvarchar(MAX)', 'real', 'since', 'smalldatetime', 'smallint', 'timestamp', 'tinyint', 'uniqueidentifier', 'varbinary', 'varchar(1)', 'varchar(10)', 'varchar(100)', 'varchar(1000)', 'varchar(1024)', 'varchar(109)', 'varchar(11)', 'varchar(12)', 'varchar(128)', 'varchar(1280)', 'varchar(14)', 'varchar(15)', 'varchar(150)', 'varchar(17)', 'varchar(18)', 'varchar(19)', 'varchar(2)', 'varchar(20)', 'varchar(200)', 'varchar(2000)', 'varchar(2048)', 'varchar(25)', 'varchar(250)', 'varchar(255)', 'varchar(256)', 'varchar(3)', 'varchar(30)', 'varchar(300)', 'varchar(32)', 'varchar(35)', 'varchar(36)', 'varchar(4)', 'varchar(40)', 'varchar(4000)', 'varchar(4096)', 'varchar(5)', 'varchar(50)', 'varchar(500)', 'varchar(5000)', 'varchar(510)', 'varchar(512)', 'varchar(550)', 'varchar(6)', 'varchar(60)', 'varchar(600)', 'varchar(64)', 'varchar(7)', 'varchar(70)', 'varchar(75)', 'varchar(8)', 'varchar(80)', 'varchar(8000)', 'varchar(9)', 'varchar(MAX)', 'xml']
