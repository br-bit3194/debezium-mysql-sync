import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# Constants
DEBEZIUM_ENDPOINT = "http://localhost:8083/connectors"
CONNECTOR_NAME = "mysql-server1-connector"
SINK_CONNECTOR_NAME = "mysql-sink-connector"
HEADERS = {"Content-Type": "application/json"}

today_date = datetime.now().strftime("%Y-%m-%d")
def setup_logger(name: str) -> logging.Logger:
    name = os.path.basename(name).replace(".py", "").strip()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    try:
        print(name)
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_path = os.path.join(log_dir, f"{name}_{today_date}.log")
        file_handler = TimedRotatingFileHandler(
            log_path, when="midnight", interval=1
        )
        file_handler.suffix = "%Y-%m-%d"
        formatter = logging.Formatter('[%(asctime)s]:%(levelname)s|%(funcName)s| %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Adding console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    except Exception as e:
        logger.error(f"Failed to setup logger: {e}")
    
    return logger
