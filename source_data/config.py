# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET', 'transaction-processing-bucket')
S3_INPUT_PREFIX = 'input/transactions/'
S3_OUTPUT_PREFIX = 'output/detections/'

# PostgreSQL Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'transaction_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

# Google Drive Configuration
GDRIVE_FOLDER_ID = '1qryhdlgNsmecWRy2haI8S3uC63wKk5X-'
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

# Processing Configuration
CHUNK_SIZE = 10000
DETECTION_BATCH_SIZE = 50
PROCESSING_INTERVAL = 1  # seconds