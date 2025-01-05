import requests
import json
from utils import setup_logger,DEBEZIUM_ENDPOINT, CONNECTOR_NAME, HEADERS, SINK_CONNECTOR_NAME

# Configure standard logger
logger = setup_logger(__file__)

def connector_exists(connector_name):
    """Check if the Debezium connector exists."""
    logger.info(f"Checking if connector {connector_name} exists.")
    response = requests.get(f"{DEBEZIUM_ENDPOINT}/{connector_name}")
    exists = response.status_code == 200
    if exists:
        logger.info(f"Connector {connector_name} exists.")
    else:
        logger.info(f"Connector {connector_name} does not exist.")
    return exists

def stop_connector(connector_name):
    """Stop the Debezium connector if it exists."""
    if connector_exists(connector_name):
        logger.info(f"Stopping connector {connector_name}.")
        response = requests.delete(f"{DEBEZIUM_ENDPOINT}/{connector_name}")
        if response.status_code == 204:
            logger.info("Connector stopped successfully.")
        else:
            logger.error(f"Failed to stop connector: {response.status_code}")
            logger.error(response.json())
    else:
        logger.info("Connector does not exist.")

def create_connector(config):
    """Create the Debezium connector with the given configuration."""
    logger.info(f"Creating connector {config['name']}.")
    response = requests.post(
        DEBEZIUM_ENDPOINT,
        headers=HEADERS,
        data=json.dumps(config),
    )
    if response.status_code == 201:
        logger.info("Debezium connector created successfully!")
    else:
        logger.error(f"Failed to create connector: {response.status_code}")
        logger.error(response.json())

def main():
    """Main function to stop and create the Debezium connector."""
    logger.info("Starting the process to stop and create the Debezium connector.")
    stop_connector(CONNECTOR_NAME)

    # MySQL connector configuration
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "mysql_server1",  # Use the Docker container name
            "database.port": "3306",
            "database.user": "root",
            "database.password": "password1",
            "database.server.id": "101",
            "database.server.name": "mysql_server1",
            "database.include.list": "customer1_db,customer2_db",
            "table.include.list": "customer1_db.user,customer2_db.user",
            "database.history.kafka.bootstrap.servers": "kafka:9092",  # Use the Docker service name
            "database.history.kafka.topic": "dbhistory.fullfillment",
            "snapshot.mode": "initial",  # Important to capture existing data
            "topic.prefix": "mysql_server1",  # Required field
            "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",  # Use the Docker service name",
            "schema.history.internal.kafka.topic": "schema-changes.mysql_server1",

            # Transformation to add the source database name and record ID
            "transforms": "AddSourceDb,AddSourceDbId",
            "transforms.AddSourceDb.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.AddSourceDb.static.field": "source_database",
            "transforms.AddSourceDb.static.value": "${database}",
            
            "transforms.AddSourceDbId.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.AddSourceDbId.static.field": "source_db_id",
            # Add the source_db_id field as a combination of source_database and record id
            "transforms.AddSourceDbId.static.value": "${database}_${record.id}"

        },
    }

    create_connector(connector_config)

if __name__ == "__main__":
    main()
