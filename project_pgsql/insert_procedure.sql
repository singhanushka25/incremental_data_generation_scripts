create publication pubkey11 for all tables;

create extension hstore;

CREATE OR REPLACE PROCEDURE insert_test_data(
    p_query TEXT,
    p_row_count INTEGER DEFAULT 1000000
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name TEXT;
    v_schema_name TEXT;
    v_columns TEXT[];
    v_types TEXT[];
    v_lengths INTEGER[];
    v_sql TEXT;
    v_values TEXT;
    v_counter INTEGER;
BEGIN
    -- Extract schema and table name from the query
    v_table_name := (regexp_matches(p_query, 'CREATE TABLE (\S+)'))[1];

    -- Check if the table name includes schema
    IF position('.' IN v_table_name) > 0 THEN
        -- Split schema and table name
        v_schema_name := split_part(v_table_name, '.', 1);
        v_table_name := split_part(v_table_name, '.', 2);
    ELSE
        -- If no schema, assume public schema
        v_schema_name := 'public';
    END IF;

    -- Debugging: output the schema and table names
    RAISE NOTICE 'Schema: %, Table: %', v_schema_name, v_table_name;

    -- Retrieve column names, data types, and character lengths (if applicable)
    SELECT array_agg(column_name ORDER BY ordinal_position),
           array_agg(data_type ORDER BY ordinal_position),
           array_agg(character_maximum_length ORDER BY ordinal_position)
    INTO v_columns, v_types, v_lengths
    FROM information_schema.columns
    WHERE table_name = v_table_name
    AND table_schema = v_schema_name;

    RAISE NOTICE 'Columns: %, Types: %, Lengths: %', v_columns, v_types, v_lengths;

    IF v_columns IS NULL OR v_types IS NULL THEN
        RAISE EXCEPTION 'Failed to retrieve column names or types for table % in schema %', v_table_name, v_schema_name;
    END IF;

    FOR v_counter IN 1..p_row_count LOOP
        v_values := '';

        FOR i IN 1..array_length(v_columns, 1) LOOP
            v_values := v_values || CASE
                WHEN v_types[i] = 'character varying' OR v_types[i] = 'text' THEN format('''%s''', left(md5(random()::text), COALESCE(v_lengths[i], 255)))
                WHEN v_types[i] = 'integer' THEN format('%s', floor(random() * 10000))
                WHEN v_types[i] = 'bigint' THEN format('%s', floor(random() * 1000000))
                WHEN v_types[i] = 'smallint' THEN format('%s', floor(random() * 32767))
                WHEN v_types[i] = 'numeric' OR v_types[i] = 'decimal' THEN format('%s', round((random() * 1000)::numeric, 2))
                WHEN v_types[i] = 'boolean' THEN CASE WHEN random() > 0.5 THEN 'TRUE' ELSE 'FALSE' END
                WHEN v_types[i] = 'date' THEN format('''%s''', (CURRENT_DATE - floor(random() * 365)::integer)::text)
                WHEN v_types[i] IN ('timestamp', 'timestamp without time zone', 'timestamp with time zone') THEN format('''%s''', (now() - (random() * interval '365 days'))::text)
                WHEN v_types[i] = 'double precision' OR v_types[i] = 'real' THEN format('%s', random() * 1000)
                WHEN v_types[i] = 'uuid' THEN 'gen_random_uuid()'
                WHEN v_types[i] IN ('json', 'jsonb') THEN format('''{"key": "%s"}''', left(md5(random()::text), COALESCE(v_lengths[i], 255)))
                WHEN v_types[i] = 'BYTEA' THEN 'decode(''DEADBEEF'', ''hex'')'
                ELSE 'NULL'
            END;

            IF i < array_length(v_columns, 1) THEN
                v_values := v_values || ', ';
            END IF;
        END LOOP;

        v_sql := format('INSERT INTO %s.%s (%s) VALUES (%s);',
                        v_schema_name, v_table_name,
                        array_to_string(v_columns, ', '),
                        v_values);

        EXECUTE v_sql;
    END LOOP;
END;
$$;