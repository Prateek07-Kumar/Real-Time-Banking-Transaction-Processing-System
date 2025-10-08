# database.py
import psycopg2
from psycopg2.extras import execute_batch
import config

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )

def init_database():
    """Initialize database tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(100) PRIMARY KEY,
            customer_id VARCHAR(100),
            customer_name VARCHAR(200),
            gender VARCHAR(10),
            merchant_id VARCHAR(100),
            transaction_type VARCHAR(50),
            transaction_amount DECIMAL(15, 2),
            transaction_date TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_importance (
            customer_id VARCHAR(100),
            transaction_type VARCHAR(50),
            weightage DECIMAL(5, 2),
            PRIMARY KEY (customer_id, transaction_type)
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_state (
            id SERIAL PRIMARY KEY,
            last_processed_row INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("""
        INSERT INTO processing_state (last_processed_row)
        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM processing_state);
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS detections (
            id SERIAL PRIMARY KEY,
            y_start_time TIMESTAMP,
            detection_time TIMESTAMP,
            pattern_id VARCHAR(20),
            action_type VARCHAR(50),
            customer_name VARCHAR(200),
            merchant_id VARCHAR(100),
            uploaded_to_s3 BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Create indexes for performance
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_customer 
        ON transactions(customer_id, merchant_id);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_merchant 
        ON transactions(merchant_id);
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_detections_uploaded 
        ON detections(uploaded_to_s3);
    """)
    
    conn.commit()
    cur.close()
    conn.close()