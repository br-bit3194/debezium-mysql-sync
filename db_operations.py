import random
from mysql.connector.pooling import MySQLConnectionPool
import pandas as pd

# Configure logging
from utils import setup_logger
from data_generate import generate_random_string, generate_random_email, generate_random_phone_number
from sqlalchemy import create_engine
logger = setup_logger(__file__)

# Database connection
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password1",
    "database": "customer1_db"
}

# Define limit variable
LIMIT = 500

# Read data from input_data.xlsx
input_data = pd.read_excel('input_data.xlsx')

engine = create_engine(f'mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}')

connection_pool = MySQLConnectionPool(
    pool_name="mypool",
    pool_size=10,  # You can adjust the pool size as per your resources
    **db_config
)

def get_db_connection():
    return connection_pool.get_connection()

def get_random_record(cursor, table):
    cursor.execute(f"SELECT * FROM {table} ORDER BY RAND() LIMIT 1")
    return cursor.fetchone()

def upsert_query(table):
    """
    Generate the upsert query string.
    Assumes 'email' is the unique key in the table for upsert.
    """
    upsert_sql = f"""
    INSERT INTO {table} (first_name, last_name, email, phone_number)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    first_name = VALUES(first_name),
    last_name = VALUES(last_name),
    phone_number = VALUES(phone_number);
    """
    return upsert_sql

def batch_upsert(df, table, conn, cursor):
    # Create a connection to the database
    # conn = engine.raw_connection()
    
    try:
        print(df)
        upsert_sql = upsert_query(table)
        
        # Loop over the DataFrame and execute the upsert in batches
        batch_size = 1000  # Adjust the batch size as needed
        for i in range(0, df.shape[0], batch_size):
            batch_data = df.iloc[i:i+batch_size].values.tolist()  # Convert the batch to list of tuples
            cursor.executemany(upsert_sql, batch_data)  # Execute upsert in batch
            
            # Commit the transaction for each batch
            conn.commit()
            
        logger.info("Upsert completed successfully.")
        return True
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        logger.error(f"Error occurred: {e}")


def edit_record(cursor, table, conn):
    batch_size = 500  # Adjust the batch size as needed
    for _ in range(0, LIMIT, batch_size):
        batch_data = []
        for _ in range(batch_size):
            record = get_random_record(cursor, table)
            if record:
                first_name = generate_random_string(6)
                last_name = record[2] + "1"
                batch_data.append((first_name, last_name, record[0]))
                logger.info(f"Queued for update in {table}: (id: {record[0]}, first_name: {first_name}, last_name: {last_name}), email: {record[3]}")
        
        if batch_data:
            update_sql = f"UPDATE {table} SET first_name = %s, last_name = %s WHERE id = %s"
            cursor.executemany(update_sql, batch_data)
            conn.commit()
            logger.info(f"Batch update executed for {len(batch_data)} records.")

def delete_record(cursor, table, conn):
    delete_sql = f"DELETE FROM {table} WHERE id = %s"
    batch_size = 500  # Adjust the batch size as needed
    for _ in range(0, LIMIT, batch_size):
        batch_data = []
        for _ in range(batch_size):
            record = get_random_record(cursor, table)
            if record:
                batch_data.append((record[0],))
                logger.info(f"Queued for deletion from {table}: (id: {record[0]}), first_name: {record[1]}, last_name: {record[2]}), email: {record[3]}")
        
        if batch_data:
            cursor.executemany(delete_sql, batch_data)
            conn.commit()
            logger.info(f"Batch delete executed for {len(batch_data)} records.")

def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    table = 'user'
    operations = {
        'insert': batch_upsert,
        'edit': edit_record,
        'delete': delete_record
    }

    while True:
        logger.info("Choose an operation: insert, edit, delete or type 'exit' to quit")
        user_input = input("Enter the operation you want to perform: ").strip().lower()

        if user_input == 'exit':
            break

        if user_input in operations:
            operation = operations[user_input]
            logger.info(f"Performing operation: {operation.__name__}")
            if user_input == 'insert':
                operation(input_data, table, conn, cursor)
            else:
                operation(cursor, table, conn)
            conn.commit()
        else:
            logger.warning("Invalid operation selected.")

    cursor.close()
    conn.close()
    logger.info("Operation completed and connection closed.")

if __name__ == "__main__":
    main()