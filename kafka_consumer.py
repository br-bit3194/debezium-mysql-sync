import json
import signal
import sys
from kafka import KafkaConsumer
import mysql.connector
from utils import setup_logger

# Configure logging
logger = setup_logger(__file__)

# Kafka consumer configuration
logger.info("Initializing Kafka consumer...")
kafka_consumer = KafkaConsumer(
    "mysql_server1.customer1_db.user",
    "mysql_server1.customer2_db.user",
    bootstrap_servers=["localhost:29092"],
    group_id="sync-group",
    auto_offset_reset="earliest",  # Start from the beginning to capture snapshots
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
)
logger.info("Kafka consumer initialized.")

# MySQL Server 2 connection details
server2_config = {
    "host": "localhost",
    "port": 3307,
    "user": "root",
    "password": "password2",
    "database": "common_db",
}

def upsert_user(cursor, data):
    logger.info(f"Upserting user with email: {data}{data['email']}")
    cursor.execute(
        """
        INSERT INTO user (first_name, last_name, email, phone_number)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        first_name=VALUES(first_name),
        last_name=VALUES(last_name),
        phone_number=VALUES(phone_number)
        """,
        (
            data["first_name"],
            data["last_name"],
            data["email"],
            data["phone_number"],
        ),
    )

# Graceful shutdown handler
def signal_handler(signum, frame):
    logger.info("Shutting down...")
    kafka_consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Connect to Server 2
try:
    logger.info("Connecting to MySQL Server 2...")
    server2_conn = mysql.connector.connect(**server2_config)
    logger.info("Connected to MySQL Server 2.")
    cursor = server2_conn.cursor()
    logger.info("Starting to process Kafka messages...")
    for message in kafka_consumer:
        logger.info("Received message from Kafka.")
        record = message.value
        if record is None:
            continue
        payload = record.get("payload")
        
        if not payload:
            continue
        
        op = payload.get("op")
        # print(payload)
        after = payload.get("after")
        before = payload.get("before")

        if op in ("c", "r", "u"):
            logger.info(f"Operation: {op}. Upserting user.")
            upsert_user(cursor, after)
        elif op == "d":
            logger.info(f"Operation: {op}. Deleting user with email: {before['email']}")
            cursor.execute("SELECT id FROM user WHERE email = %s", (before["email"],))
            user_id = cursor.fetchone()
            if user_id:
                cursor.execute("DELETE FROM user WHERE id = %s", (user_id[0],))
        
        server2_conn.commit()
        logger.info("Database operation committed.")
    kafka_consumer.commit()
    cursor.close()
    server2_conn.close()
except Exception as e:
    logger.error(f"Error: {e}")
    if cursor:
        cursor.close()
    if server2_conn:
        server2_conn.close()
