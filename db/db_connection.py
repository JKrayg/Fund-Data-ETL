import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

def connect():
    return mysql.connector.connect(
        user=os.getenv('USER'), password=os.getenv('PASSWD'),
        host=os.getenv('HOST'), database=os.getenv('DB_NAME'))