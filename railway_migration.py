import psycopg2
from psycopg2 import sql
from io import BytesIO
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

load_dotenv()

def parse_db_url(url):
    """Parse database URL into connection parameters"""
    parsed = urlparse(url)
    return {
        'dbname': parsed.path[1:],
        'user': parsed.username,
        'password': parsed.password,
        'host': parsed.hostname,
        'port': parsed.port
    }

def get_schema(cursor):
    """Quickly get list of tables"""
    cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
    """)
    return [row[0] for row in cursor.fetchall()]

def create_schema(source_cur, dest_cur):
    """Fast schema creation with table replacement"""
    source_cur.execute("""
        SELECT 'DROP TABLE IF EXISTS ' || quote_ident(table_name) || ' CASCADE; ' ||
               'CREATE TABLE ' || quote_ident(table_name) || ' (' || 
               string_agg(column_def, ', ') || ');' AS ddl
        FROM (
            SELECT 
                table_name,
                quote_ident(column_name) || ' ' || 
                CASE 
                    WHEN data_type = 'USER-DEFINED' THEN udt_name
                    ELSE data_type
                END ||
                CASE 
                    WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length::text || ')'
                    ELSE ''
                END || 
                CASE 
                    WHEN is_nullable = 'NO' THEN ' NOT NULL'
                    ELSE ''
                END AS column_def
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        ) AS columns
        GROUP BY table_name
    """)
    
    for row in source_cur:
        dest_cur.execute(row[0])

def copy_data(source_cur, dest_cur, table):
    """High-speed data transfer with conflict-free COPY"""
    try:
        buffer = BytesIO()
        
        # Export from source
        copy_from_query = sql.SQL("COPY (SELECT * FROM {}) TO STDOUT WITH BINARY").format(sql.Identifier(table))
        source_cur.copy_expert(copy_from_query, buffer)
        
        # Reset and import to destination
        buffer.seek(0)
        copy_to_query = sql.SQL("COPY {} FROM STDIN WITH BINARY").format(sql.Identifier(table))
        dest_cur.copy_expert(copy_to_query, buffer)
        
        print(f"Successfully copied {table}")

    except Exception as e:
        print(f"Error copying table {table}: {str(e)}")
        raise
    finally:
        buffer.close()

def migrate_database(source_url, destination_url):
    """Robust migration function with full schema refresh"""
    source_conn = dest_conn = None
    try:
        # Parse connection parameters
        source_params = parse_db_url(source_url)
        dest_params = parse_db_url(destination_url)
        
        # Establish connections
        print("Connecting to databases...")
        source_conn = psycopg2.connect(**source_params)
        dest_conn = psycopg2.connect(**dest_params)
        
        # Set autocommit
        source_conn.autocommit = True
        dest_conn.autocommit = True
        
        with source_conn.cursor() as source_cur, dest_conn.cursor() as dest_cur:
            # Force schema recreation
            print("Recreating schema...")
            create_schema(source_cur, dest_cur)
            
            # Get tables
            print("Getting table list...")
            tables = get_schema(source_cur)
            print(f"Found {len(tables)} tables to migrate")
            
            # Copy data
            print("\nStarting bulk data copy...")
            for table in tables:
                print(f"Copying {table}...")
                copy_data(source_cur, dest_cur, table)
            
        print("\nMigration completed successfully!")

    except Exception as e:
        print(f"\nMigration failed: {str(e)}")
        raise
    finally:
        if source_conn: source_conn.close()
        if dest_conn: dest_conn.close()

if __name__ == "__main__":
    
    SOURCE_DB_URL =  os.getenv('SOURCE_DB_URL')
    DEST_DB_URL = os.getenv('DEST_DB_URL')
    
    if not SOURCE_DB_URL or not DEST_DB_URL:
        raise ValueError("Database URLs not found in environment variables")

    migrate_database(SOURCE_DB_URL, DEST_DB_URL)
