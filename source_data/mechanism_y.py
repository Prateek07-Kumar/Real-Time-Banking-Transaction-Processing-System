# mechanism_y.py
"""
Mechanism Y: Ingests S3 transaction chunks, detects patterns, and uploads detections
"""
import time
import pandas as pd
from datetime import datetime
import pytz
import database
import s3_handler
import config

class MechanismY:
    def __init__(self):
        self.processed_files = set()
        self.y_start_time = None
        
    def get_ist_time(self):
        """Get current time in IST"""
        ist = pytz.timezone('Asia/Kolkata')
        return datetime.now(ist).replace(tzinfo=None)
    
    def process_transaction_chunk(self, s3_key):
        """Process a single transaction chunk from S3"""
        print(f"Processing file: {s3_key}")
        
        # Download and parse
        chunk_df = s3_handler.download_s3_file_to_dataframe(s3_key)
        
        # Store in database
        transactions_data = []
        for _, row in chunk_df.iterrows():
            transactions_data.append((
                str(row.get('TransactionId', '')),
                str(row.get('CustomerId', '')),
                str(row.get('CustomerName', '')),
                str(row.get('Gender', '')),
                str(row.get('MerchantId', '')),
                str(row.get('TransactionType', '')),
                float(row.get('TransactionAmount', 0)),
                pd.to_datetime(row.get('TransactionDate', datetime.now()))
            ))
        
        database.insert_transactions(transactions_data)
        print(f"Inserted {len(transactions_data)} transactions into database")
    
    def detect_pattern_1(self):
        """
        Pattern 1: Customer in top 10 percentile for transactions with bottom 10% weight
        Action: UPGRADE
        Only when merchant has >50K transactions
        """
        conn = database.get_db_connection()
        cur = conn.cursor()
        
        query = """
        WITH merchant_stats AS (
            SELECT 
                merchant_id,
                COUNT(*) as total_transactions
            FROM transactions
            GROUP BY merchant_id
            HAVING COUNT(*) > 50000
        ),
        customer_merchant_stats AS (
            SELECT 
                t.customer_id,
                t.customer_name,
                t.merchant_id,
                t.transaction_type,
                COUNT(*) as transaction_count,
                COALESCE(ci.weightage, 0) as weightage
            FROM transactions t
            LEFT JOIN customer_importance ci 
                ON t.customer_id = ci.customer_id 
                AND t.transaction_type = ci.transaction_type
            WHERE t.merchant_id IN (SELECT merchant_id FROM merchant_stats)
            GROUP BY t.customer_id, t.customer_name, t.merchant_id, 
                     t.transaction_type, ci.weightage
        ),
        customer_avg_weight AS (
            SELECT 
                customer_id,
                customer_name,
                merchant_id,
                SUM(transaction_count) as total_transactions,
                AVG(weightage) as avg_weightage
            FROM customer_merchant_stats
            GROUP BY customer_id, customer_name, merchant_id
        ),
        merchant_percentiles AS (
            SELECT 
                merchant_id,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_transactions) as tx_90th,
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY avg_weightage) as weight_10th
            FROM customer_avg_weight
            GROUP BY merchant_id
        )
        SELECT DISTINCT
            caw.customer_name,
            caw.merchant_id
        FROM customer_avg_weight caw
        JOIN merchant_percentiles mp ON caw.merchant_id = mp.merchant_id
        WHERE caw.total_transactions >= mp.tx_90th
          AND caw.avg_weightage <= mp.weight_10th
          AND NOT EXISTS (
              SELECT 1 FROM detections d
              WHERE d.pattern_id = 'PatId1'
                AND d.customer_name = caw.customer_name
                AND d.merchant_id = caw.merchant_id
          )
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        detections = []
        detection_time = self.get_ist_time()
        
        for customer_name, merchant_id in results:
            detection = (
                self.y_start_time,
                detection_time,
                'PatId1',
                'UPGRADE',
                customer_name,
                merchant_id
            )
            database.insert_detection(detection)
            detections.append(detection)
        
        cur.close()
        conn.close()
        
        if detections:
            print(f"Pattern 1: Detected {len(detections)} UPGRADE cases")
        
        return detections
    
    def detect_pattern_2(self):
        """
        Pattern 2: Customer with avg transaction < 23 and >= 80 transactions
        Action: CHILD
        """
        conn = database.get_db_connection()
        cur = conn.cursor()
        
        query = """
        SELECT 
            t.customer_name,
            t.merchant_id,
            AVG(t.transaction_amount) as avg_amount,
            COUNT(*) as transaction_count
        FROM transactions t
        GROUP BY t.customer_name, t.merchant_id
        HAVING COUNT(*) >= 80
          AND AVG(t.transaction_amount) < 23
          AND NOT EXISTS (
              SELECT 1 FROM detections d
              WHERE d.pattern_id = 'PatId2'
                AND d.customer_name = t.customer_name
                AND d.merchant_id = t.merchant_id
          )
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        detections = []
        detection_time = self.get_ist_time()
        
        for customer_name, merchant_id, avg_amount, tx_count in results:
            detection = (
                self.y_start_time,
                detection_time,
                'PatId2',
                'CHILD',
                customer_name,
                merchant_id
            )
            database.insert_detection(detection)
            detections.append(detection)
        
        cur.close()
        conn.close()
        
        if detections:
            print(f"Pattern 2: Detected {len(detections)} CHILD cases")
        
        return detections
    
    def detect_pattern_3(self):
        """
        Pattern 3: Merchants with more male than female customers (female > 100)
        Action: DEI-NEEDED
        """
        conn = database.get_db_connection()
        cur = conn.cursor()
        
        query = """
        WITH gender_counts AS (
            SELECT 
                merchant_id,
                SUM(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN UPPER(gender) = 'MALE' THEN 1 ELSE 0 END) as male_count
            FROM (
                SELECT DISTINCT merchant_id, customer_id, gender
                FROM transactions
            ) unique_customers
            GROUP BY merchant_id
        )
        SELECT merchant_id
        FROM gender_counts
        WHERE female_count > 100
          AND male_count > female_count
          AND NOT EXISTS (
              SELECT 1 FROM detections d
              WHERE d.pattern_id = 'PatId3'
                AND d.merchant_id = gender_counts.merchant_id
          )
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        detections = []
        detection_time = self.get_ist_time()
        
        for (merchant_id,) in results:
            detection = (
                self.y_start_time,
                detection_time,
                'PatId3',
                'DEI-NEEDED',
                '',  # No customer name for merchant-level pattern
                merchant_id
            )
            database.insert_detection(detection)
            detections.append(detection)
        
        cur.close()
        conn.close()
        
        if detections:
            print(f"Pattern 3: Detected {len(detections)} DEI-NEEDED cases")
        
        return detections
    
    def detect_all_patterns(self):
        """Run all pattern detections"""
        all_detections = []
        
        all_detections.extend(self.detect_pattern_1())
        all_detections.extend(self.detect_pattern_2())
        all_detections.extend(self.detect_pattern_3())
        
        return all_detections
    
    def upload_detection_batches(self):
        """Upload pending detections to S3 in batches"""
        while True:
            detections = database.get_unuploaded_detections(config.DETECTION_BATCH_SIZE)
            
            if not detections:
                break
            
            # Upload to S3
            s3_handler.upload_detections_to_s3(detections)
            
            # Mark as uploaded
            detection_ids = [d[0] for d in detections]
            database.mark_detections_uploaded(detection_ids)
    
    def run(self):
        """Main execution loop"""
        print("Starting Mechanism Y...")
        self.y_start_time = self.get_ist_time()
        
        while True:
            try:
                # List files in S3
                s3_files = s3_handler.list_s3_transaction_files()
                
                # Process new files
                new_files = [f for f in s3_files if f not in self.processed_files]
                
                for s3_key in new_files:
                    # Process transaction chunk
                    self.process_transaction_chunk(s3_key)
                    self.processed_files.add(s3_key)
                    
                    # Detect patterns
                    self.detect_all_patterns()
                    
                    # Upload detections
                    self.upload_detection_batches()
                
                # Wait before checking again
                time.sleep(config.PROCESSING_INTERVAL)
                
            except KeyboardInterrupt:
                print("\nMechanism Y stopped by user")
                break
            except Exception as e:
                print(f"Error in Mechanism Y: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(config.PROCESSING_INTERVAL)
                
            except KeyboardInterrupt:
                print("\nMechanism X stopped by user")
                break
            except Exception as e:
                print(f"Error in Mechanism X: {e}")
                time.sleep(config.PROCESSING_INTERVAL)

def get_customer_importance_file_id(service):
    """Get the file ID for CustomerImportance.csv"""
    files = list_files_in_folder(service, config.GDRIVE_FOLDER_ID)
    
    for file in files:
        if file['name'].lower() == 'customerimportance.csv':
            return file['id']
    
    return None
    print("Database initialized successfully")

def insert_transactions(transactions_data):
    """Insert transaction data into database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        INSERT INTO transactions 
        (transaction_id, customer_id, customer_name, gender, merchant_id, 
        transaction_type, transaction_amount, transaction_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    
    execute_batch(cur, query, transactions_data)
    conn.commit()
    cur.close()
    conn.close()

def insert_customer_importance(importance_data):
    """Insert customer importance data into database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        INSERT INTO customer_importance 
        (customer_id, transaction_type, weightage)
        VALUES (%s, %s, %s)
        ON CONFLICT (customer_id, transaction_type) 
        DO UPDATE SET weightage = EXCLUDED.weightage;
    """
    
    execute_batch(cur, query, importance_data)
    conn.commit()
    cur.close()
    conn.close()

def get_last_processed_row():
    """Get the last processed row number"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT last_processed_row FROM processing_state ORDER BY id DESC LIMIT 1")
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else 0

def update_last_processed_row(row_number):
    """Update the last processed row number"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE processing_state 
        SET last_processed_row = %s, updated_at = CURRENT_TIMESTAMP 
        WHERE id = (SELECT id FROM processing_state ORDER BY id DESC LIMIT 1)
    """, (row_number,))
    conn.commit()
    cur.close()
    conn.close()

def insert_detection(detection_data):
    """Insert detection data into database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        INSERT INTO detections 
        (y_start_time, detection_time, pattern_id, action_type, customer_name, merchant_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    cur.execute(query, detection_data)
    conn.commit()
    cur.close()
    conn.close()

def get_unuploaded_detections(limit=50):
    """Get detections that haven't been uploaded to S3"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, y_start_time, detection_time, pattern_id, 
            action_type, customer_name, merchant_id
        FROM detections
        WHERE uploaded_to_s3 = FALSE
        ORDER BY created_at
        LIMIT %s
    """, (limit,))
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def mark_detections_uploaded(detection_ids):
    """Mark detections as uploaded to S3"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE detections 
        SET uploaded_to_s3 = TRUE 
        WHERE id = ANY(%s)
    """, (detection_ids,))
    
    conn.commit()
    cur.close()
    conn.close()