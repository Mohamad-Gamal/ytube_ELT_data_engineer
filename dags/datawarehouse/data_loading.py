import os
import json
from datatime import date
import logging

logging = logging.getLogger(__name__)


def load_path():
    file_path =  f"./data/YT_data_{date.today()}.json"
    try:
        if os.path.exists(file_path):
            logging.info(f"File found: {file_path}")
            return file_path
        else:
            logging.error(f"File not found: {file_path}")
            return None
    except Exception as e:
        logging.error(f"Error checking file path: {e}")
        return None
    
def load_data_from_api():
    file_path = load_path()
    try:
        logging.info(f"Loading data from file: {file_path}")
        with open(file_path, 'r', encoding="utf-8") as file:
            data = json.load(file)
            logging.info(f"Data loaded successfully from {file_path}")
            return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {file_path}")
        raise