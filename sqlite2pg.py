import sqlite3
import psycopg2
from psycopg2.extras import execute_values

SQLITE_DB = '/var/lib/headscale/db.sqlite'
PG_CONNINFO = "dbname=headscale user=headscale password=your_password host=localhost"

# Описание структуры таблиц: названия, типы, индексы
TABLES = {
    'migrations': {
        'columns': [
            ('id', 'varchar(32) PRIMARY KEY')
        ]
    },
    'users': {
        'columns': [
            ('id', 'bigserial PRIMARY KEY'),
            ('created_at', 'timestamp with time zone'),
            ('updated_at', 'timestamp with time zone'),
            ('deleted_at', 'timestamp with time zone'),
            ('name', 'varchar(63) UNIQUE NOT NULL'),
            ('display_name', 'varchar(63)'),
            ('email', 'varchar(255)'),
            ('provider_identifier', 'varchar(255)'),
            ('provider', 'varchar(255)'),
            ('profile_pic_url', 'varchar(255)')
        ]
    },
    'pre_auth_keys': {
        'columns': [
            ('id', 'bigserial PRIMARY KEY'),
            ('key', 'varchar(63) UNIQUE NOT NULL'),
            ('user_id', 'bigint REFERENCES users(id)'),
            ('reusable', 'boolean DEFAULT false'),
            ('ephemeral', 'boolean DEFAULT false'),
            ('used', 'boolean DEFAULT false'),
            ('tags', 'varchar(255)'),
            ('created_at', 'timestamp with time zone'),
            ('expiration', 'timestamp with time zone')
        ]
    },
    'api_keys': {
        'columns': [
            ('id', 'bigserial PRIMARY KEY'),
            ('prefix', 'varchar(8) UNIQUE NOT NULL'),
            ('hash', 'bytea NOT NULL'),
            ('created_at', 'timestamp with time zone'),
            ('expiration', 'timestamp with time zone'),
            ('last_seen', 'timestamp with time zone')
        ]
    },
    'policies': {
        'columns': [
            ('id', 'bigserial PRIMARY KEY'),
            ('created_at', 'timestamp with time zone'),
            ('updated_at', 'timestamp with time zone'),
            ('deleted_at', 'timestamp with time zone'),
            ('data', 'text')
        ]
    },
    'nodes': {
        'columns': [
            ('id', 'bigserial PRIMARY KEY'),
            ('machine_key', 'varchar(255) UNIQUE NOT NULL'),
            ('node_key', 'varchar(255)'),
            ('disco_key', 'varchar(255)'),
            ('endpoints', 'text'),
            ('host_info', 'text'),
            ('ipv4', 'inet'),
            ('ipv6', 'inet'),
            ('hostname', 'varchar(255)'),
            ('given_name', 'varchar(255)'),
            ('user_id', 'bigint REFERENCES users(id)'),
            ('register_method', 'varchar(255)'),
            ('forced_tags', 'text'),
            ('auth_key_id', 'bigint'),
            ('expiry', 'timestamp with time zone'),
            ('last_seen', 'timestamp with time zone'),
            ('approved_routes', 'text'),
            ('created_at', 'timestamp with time zone'),
            ('updated_at', 'timestamp with time zone'),
            ('deleted_at', 'timestamp with time zone')
        ]
    }
}

BOOL_FIELDS = {
    'pre_auth_keys': ['reusable', 'ephemeral', 'used'],
}

BLOB_FIELDS = {
    'api_keys': ['hash'],
}

def convert_bool(val):
    if val is None:
        return None
    return bool(val)

def convert_blob(val):
    return val  # sqlite3 returns bytes for blob

def create_table(pg_cur, table, table_def):
    columns = ',\n  '.join([f"{col} {dtype}" for col, dtype in table_def['columns']])
    sql = f'CREATE TABLE IF NOT EXISTS {table} (\n  {columns}\n);'
    try:
        pg_cur.execute(sql)
        print(f"[{table}] Table created or already exists.")
    except Exception as e:
        print(f"[{table}] Error creating table: {e}")

def fetch_and_insert(sqlite_cur, pg_cur, table, fields, bool_fields=None, blob_fields=None):
    sqlite_cur.execute(f"SELECT {', '.join(fields)} FROM {table}")
    rows = []
    for row in sqlite_cur.fetchall():
        row = list(row)
        if bool_fields:
            for idx, fname in enumerate(fields):
                if fname in bool_fields:
                    row[idx] = convert_bool(row[idx])
        if blob_fields:
            for idx, fname in enumerate(fields):
                if fname in blob_fields and row[idx] is not None:
                    row[idx] = bytes(row[idx])
        if len(fields) == 1:
            rows.append((row[0],))
        else:
            rows.append(tuple(row))
    if not rows:
        print(f'[{table}] No data to insert.')
        return
    sql = f'INSERT INTO {table} ({", ".join(fields)}) VALUES %s'
    try:
        execute_values(pg_cur, sql, rows)
        print(f'[{table}] Imported {len(rows)} rows.')
    except Exception as e:
        print(f'[{table}] Error: {e}')

def update_sequences(pg_cur):
    print("Updating PostgreSQL sequences...")
    seq_sql = """
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN
            SELECT
                pg_class.relname AS table_name,
                pg_attribute.attname AS column_name,
                pg_get_serial_sequence(pg_class.relname, pg_attribute.attname) AS seq
            FROM
                pg_class
                JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
            WHERE
                pg_class.relkind = 'r'
                AND pg_namespace.nspname = 'public'
                AND pg_attribute.attnum > 0
                AND pg_get_serial_sequence(pg_class.relname, pg_attribute.attname) IS NOT NULL
        LOOP
            EXECUTE format(
                'SELECT setval(''%s'', COALESCE((SELECT MAX(%s) FROM %I), 1))',
                r.seq, r.column_name, r.table_name
            );
        END LOOP;
    END$$;
    """
    try:
        pg_cur.execute(seq_sql)
        print("Sequences updated successfully.")
    except Exception as e:
        print(f"Error updating sequences: {e}")

def main():
    sqlite = sqlite3.connect(SQLITE_DB)
    sqlite.row_factory = sqlite3.Row
    pg = psycopg2.connect(PG_CONNINFO)
    pg.autocommit = True
    sqlite_cur = sqlite.cursor()
    pg_cur = pg.cursor()

    order = ['users', 'migrations', 'pre_auth_keys', 'api_keys', 'policies', 'nodes']

    # Step 1: Create tables if not exist
    print("Creating tables in PostgreSQL...")
    for table in order:
        create_table(pg_cur, table, TABLES[table])

    # Step 2: Transfer data
    for table in order:
        print(f'Processing table: {table}...')
        bool_fields = BOOL_FIELDS.get(table, [])
        blob_fields = BLOB_FIELDS.get(table, [])
        fields = [col for col, _ in TABLES[table]['columns']]
        fetch_and_insert(sqlite_cur, pg_cur, table, fields, bool_fields, blob_fields)

    # Step 3: Update sequences
    update_sequences(pg_cur)

    pg_cur.close()
    pg.close()
    sqlite_cur.close()
    sqlite.close()
    print('Migration completed successfully!')

if __name__ == '__main__':
    main()
