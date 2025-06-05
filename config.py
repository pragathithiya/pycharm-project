import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
# config.py
DB_URL = 'postgresql://postgres:your_password@localhost:5432/your_database'
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'your_database'),
    'username': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password')
}