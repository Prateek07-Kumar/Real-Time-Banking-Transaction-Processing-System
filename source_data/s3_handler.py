# s3_handler.py
import boto3
import json
import csv
import io
from datetime import datetime
import config

def get_s3_client():
    """Create and return S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION
    )

def upload_transactions_to_s3(transactions_df, chunk_number):
    """Upload transaction chunk to S3"""
    s3_client = get_s3_client()
    
    # Convert DataFrame to CSV
    csv_buffer = io.StringIO()
    transactions_df.to_csv(csv_buffer, index=False)
    
    # Generate unique filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{config.S3_INPUT_PREFIX}chunk_{chunk_number}_{timestamp}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=config.S3_BUCKET,
        Key=filename,
        Body=csv_buffer.getvalue()
    )
    
    print(f"Uploaded chunk {chunk_number} to S3: {filename}")
    return filename

def upload_detections_to_s3(detections):
    """Upload detections to S3"""
    s3_client = get_s3_client()
    
    # Convert detections to CSV format
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    
    # Write header
    writer.writerow(['YStartTime(IST)', 'DetectionTime(IST)', 'PatternId', 
                    'ActionType', 'CustomerName', 'MerchantId'])
    
    # Write data
    for detection in detections:
        _, y_start, detect_time, pattern_id, action_type, cust_name, merchant_id = detection
        
        # Format timestamps to IST
        y_start_str = y_start.strftime('%Y-%m-%d %H:%M:%S') if y_start else ''
        detect_time_str = detect_time.strftime('%Y-%m-%d %H:%M:%S') if detect_time else ''
        
        writer.writerow([
            y_start_str,
            detect_time_str,
            pattern_id or '',
            action_type or '',
            cust_name or '',
            merchant_id or ''
        ])
    
    # Generate unique filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"{config.S3_OUTPUT_PREFIX}detections_{timestamp}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=config.S3_BUCKET,
        Key=filename,
        Body=csv_buffer.getvalue()
    )
    
    print(f"Uploaded {len(detections)} detections to S3: {filename}")
    return filename

def list_s3_transaction_files():
    """List all transaction files in S3"""
    s3_client = get_s3_client()
    
    response = s3_client.list_objects_v2(
        Bucket=config.S3_BUCKET,
        Prefix=config.S3_INPUT_PREFIX
    )
    
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            files.append(obj['Key'])
    
    return files

def download_s3_file_to_dataframe(s3_key):
    """Download S3 file and convert to DataFrame"""
    s3_client = get_s3_client()
    
    obj = s3_client.get_object(Bucket=config.S3_BUCKET, Key=s3_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    return df
