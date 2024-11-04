import logging
import sys

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[logging.StreamHandler(sys.stdout)]  # Output to stdout
)


def get_logger(name: str):
    return logging.getLogger(name)
