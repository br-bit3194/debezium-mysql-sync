from typing import List, Tuple
import mysql.connector
from mysql.connector import MySQLConnection
from utils import setup_logger

# Configure standard logger
logger = setup_logger(__file__)

# Connection details for MySQL Server 1 (Docker)
server1_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password1",
}

# Connection details for MySQL Server 2 (Docker)
server2_config = {
    "host": "localhost",
    "port": 3307,
    "user": "root",
    "password": "password2",
}

def create_and_populate_db(connection: MySQLConnection, db_name: str, records: List[Tuple[str, str, str, str]], add_source_name_id: bool = False) -> None:
    try:
        with connection.cursor() as cursor:
            logger.info(f"Creating database {db_name}")
            
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")

            # Create user table
            create_table_query = """
                CREATE TABLE IF NOT EXISTS user (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100) UNIQUE,
                    phone_number VARCHAR(15)
            """
            # if add_source_name_id:
            #     create_table_query += ", sourceName_id INT"
            create_table_query += ")"

            cursor.execute(create_table_query)
            logger.info(f"Table 'user' created in database {db_name}")

            # Insert sample data
            if records:
                cursor.executemany(
                    "INSERT INTO user (first_name, last_name, email, phone_number) VALUES (%s, %s, %s, %s)",
                    records,
                )
                logger.info(f"Inserted {len(records)} records into table 'user' in database {db_name}")
            
            connection.commit()
    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        connection.rollback()

def main() -> None:
    try:
        # Connect to MySQL Server 1
        with mysql.connector.connect(**server1_config) as server1:
            logger.info("Connected to MySQL Server 1")
            create_and_populate_db(
                server1,
                "customer1_db",
                [
                    ("John", "Doe", "john.doe@example.com", "1234567890"),
                    ("Jane", "Smith", "jane.smith@example.com", "2345678901"),
                    ("Charlie", "Davis", "charlie.davis@example.com", "5678901234"),
                    ("Diana", "Miller", "diana.miller@example.com", "6789012345")
                ],
            )
            create_and_populate_db(
                server1,
                "customer2_db",
                [
                    ("Alice", "Johnson", "alice.johnson@example.com", "3456789012"),
                    ("Bob", "Brown", "bob.brown@example.com", "4567890123"),
                    ("Eve", "White", "eve.white@example.com", "7890123456"),
                    ("Frank", "Green", "frank.green@example.com", "8901234567"),
                    ("Grace", "Black", "grace.black@example.com", "9012345678")
                ],
            )
            logger.info("Closed connection to MySQL Server 1")

        # Connect to MySQL Server 2
        with mysql.connector.connect(**server2_config) as server2:
            logger.info("Connected to MySQL Server 2")
            create_and_populate_db(
                server2,
                "common_db",
                [],  # No initial data for common_db
                add_source_name_id=True
            )
            logger.info("Closed connection to MySQL Server 2")

        logger.info("Databases and tables created successfully.")
    except mysql.connector.Error as err:
        logger.error(f"Connection error: {err}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
