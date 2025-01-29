# PostgreSQL Fast Migration Tool

A high-performance Python tool for migrating PostgreSQL databases with automatic schema recreation and efficient bulk data transfer.

## Features

- Full schema migration with automatic table recreation
- High-speed binary data transfer using COPY commands
- Handles all PostgreSQL data types including user-defined types
- Automatic table dependency resolution with CASCADE
- Connection parameter parsing from database URLs
- Environment variable configuration for secure credential management

## Requirements

- Python 3.x
- psycopg2
- python-dotenv
- PostgreSQL (source and destination servers)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/kkrajid/pg-schema-migrator.git
cd pg-schema-migrator
```

2. Install required packages:
```bash
pip install psycopg2 python-dotenv
```

3. Create a `.env` file in the project root and add your database URLs:
```env
SOURCE_DB_URL=postgresql://user:password@host:port/dbname
DEST_DB_URL=postgresql://user:password@host:port/dbname
```

## Usage

1. Configure your database connections in the `.env` file
2. Run the migration script:
```bash
python railway_migration.py
```

The script will:
- Connect to both databases
- Recreate the schema in the destination database
- Copy all data using high-speed binary transfer
- Display progress for each table being migrated

## How It Works

1. **Schema Analysis**: Extracts the complete schema from the source database including tables, columns, and data types
2. **Schema Recreation**: Drops and recreates tables in the destination database to ensure clean migration
3. **Data Transfer**: Uses PostgreSQL's binary COPY command for maximum throughput
4. **Error Handling**: Includes robust error handling and connection management

## Function Overview

- `parse_db_url(url)`: Parses database URLs into connection parameters
- `get_schema(cursor)`: Retrieves list of tables from database
- `create_schema(source_cur, dest_cur)`: Recreates complete schema in destination
- `copy_data(source_cur, dest_cur, table)`: Performs high-speed table data copy
- `migrate_database(source_url, destination_url)`: Main migration function

## Safety Features

- Automatic connection closing in case of errors
- Binary data handling for all PostgreSQL types
- Credential management through environment variables
- Table existence checks before recreation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

- Always backup your database before performing migrations
- Test the migration process in a non-production environment first
- Verify data integrity after migration

## Support

For issues or questions, please open an issue in the GitHub repository.
