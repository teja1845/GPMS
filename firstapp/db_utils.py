import psycopg2
import logging

# Configure logging
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establish a direct connection to the PostgreSQL database."""
    try:
        logger.debug("Attempting to establish a database connection...")
        conn = psycopg2.connect(
            dbname="22CS10009",   # Database Name
            user="postgres",      # Database Owner/User
            password="Teja@1845",  # Set your actual PostgreSQL password
            host="10.145.112.157",     # Change this if connecting to a remote server
            port="5433"           # Default PostgreSQL port
        )
        logger.debug("Database connection successful!")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None
