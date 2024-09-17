import mysql.connector
from mysql.connector import Error

def get_tables_and_columns(connection):
    tables_columns = {}
    cursor = connection.cursor()

    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]        
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`;")
        columns = cursor.fetchall()        
        tables_columns[table_name] = [column[0] for column in columns]

    return tables_columns

def compare_databases(db1_tables, db2_tables):
    db1_table_set = set(db1_tables.keys())
    db2_table_set = set(db2_tables.keys())

    missing_in_db2 = db1_table_set - db2_table_set
    missing_in_db1 = db2_table_set - db1_table_set

    common_tables = db1_table_set & db2_table_set

    column_differences = {}
    for table in common_tables:
        db1_columns = set(db1_tables[table])
        db2_columns = set(db2_tables[table])

        missing_columns_in_db2 = db1_columns - db2_columns
        missing_columns_in_db1 = db2_columns - db1_columns

        if missing_columns_in_db2 or missing_columns_in_db1:
            column_differences[table] = {
                "missing_in_db1": missing_columns_in_db1,
                "missing_in_db2": missing_columns_in_db2
            }

    return {
        "missing_tables_in_db2": missing_in_db2,
        "missing_tables_in_db1": missing_in_db1,
        "column_differences": column_differences
    }

def print_differences(differences):
    print("Missing tables in DB2:", differences["missing_tables_in_db2"])
    print("Missing tables in DB1:", differences["missing_tables_in_db1"])
    
    print("\nColumn differences:")
    for table, diff in differences["column_differences"].items():
        print(f"\nTable: {table}")
        print("Missing columns in DB1:", diff["missing_in_db1"])
        print("Missing columns in DB2:", diff["missing_in_db2"])

def main():
    try:
        connection1 = mysql.connector.connect(
            host='host',
            database='database',
            user='root',
            password='root'
        )

        connection2 = mysql.connector.connect(
            host='host',
            database='database',
            user='root',
            password='root'
        )

        if connection1.is_connected() and connection2.is_connected():
            print("Connected to both databases")

            # Get tables and columns from both databases
            db1_tables = get_tables_and_columns(connection1)
            db2_tables = get_tables_and_columns(connection2)

            differences = compare_databases(db1_tables, db2_tables)

            print_differences(differences)

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection1.is_connected():
            connection1.close()
        if connection2.is_connected():
            connection2.close()

if __name__ == "__main__":
    main()