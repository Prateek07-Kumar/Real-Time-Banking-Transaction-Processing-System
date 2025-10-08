# mechanism_x.py
"""
Mechanism X: Reads transactions from Google Drive and uploads chunks to S3 every second
"""
import time
import pandas as pd
from datetime import datetime
import database
import gdrive_handler
import s3_handler
import config

class MechanismX:
    def __init__(self):
        self.service = gdrive_handler.get_gdrive_service()
        self.transactions_df = None
        self.chunk_number = 0
        
    def load_initial_data(self):
        """Load transactions and customer importance data"""
        print("Loading initial data from Google Drive...")
        
        # Load transactions
        trans_file_id = gdrive_handler.get_transactions_file_id(self.service)
        if not trans_file_id:
            raise Exception("transactions.csv not found in Google Drive folder")
        
        self.transactions_df = gdrive_handler.download_csv_from_gdrive(
            self.service, trans_file_id
        )
        print(f"Loaded {len(self.transactions_df)} transactions")
        
        # Load customer importance
        importance_file_id = gdrive_handler.get_customer_importance_file_id(self.service)
        if importance_file_id:
            importance_df = gdrive_handler.download_csv_from_gdrive(
                self.service, importance_file_id
            )
            
            # Store in database
            importance_data = [
                (row['CustomerId'], row['TransactionType'], row['Weightage'])
                for _, row in importance_df.iterrows()
            ]
            database.insert_customer_importance(importance_data)
            print(f"Loaded {len(importance_df)} customer importance records")
    
    def process_next_chunk(self):
        """Process and upload next chunk of transactions"""
        last_row = database.get_last_processed_row()
        
        # Check if we have more data to process
        if last_row >= len(self.transactions_df):
            print("All transactions have been processed")
            return False
        
        # Get next chunk
        start_idx = last_row
        end_idx = min(start_idx + config.CHUNK_SIZE, len(self.transactions_df))
        chunk_df = self.transactions_df.iloc[start_idx:end_idx].copy()
        
        # Upload to S3
        self.chunk_number += 1
        s3_handler.upload_transactions_to_s3(chunk_df, self.chunk_number)
        
        # Update processing state
        database.update_last_processed_row(end_idx)
        
        print(f"Processed chunk {self.chunk_number}: rows {start_idx} to {end_idx}")
        return True
    
    def run(self):
        """Main execution loop"""
        print("Starting Mechanism X...")
        
        # Load initial data
        self.load_initial_data()
        
        # Process chunks every second
        while True:
            try:
                has_more = self.process_next_chunk()
                
                if not has_more:
                    print("Mechanism X completed processing all transactions")
                    break
                
                # Wait for next second
                time.sleep(config.PROCESSING_INTERVAL)