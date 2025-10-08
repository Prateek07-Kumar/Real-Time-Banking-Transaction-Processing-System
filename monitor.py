# monitor.py
"""
Monitoring and dashboard script for the transaction processing system
"""
import database
import s3_handler
import time
from datetime import datetime
import psycopg2

def get_system_stats():
    """Get current system statistics"""
    conn = database.get_db_connection()
    cur = conn.cursor()
    
    stats = {}
    
    # Total transactions processed
    cur.execute("SELECT COUNT(*) FROM transactions")
    stats['total_transactions'] = cur.fetchone()[0]
    
    # Unique customers
    cur.execute("SELECT COUNT(DISTINCT customer_id) FROM transactions")
    stats['unique_customers'] = cur.fetchone()[0]
    
    # Unique merchants
    cur.execute("SELECT COUNT(DISTINCT merchant_id) FROM transactions")
    stats['unique_merchants'] = cur.fetchone()[0]
    
    # Last processed row
    cur.execute("SELECT last_processed_row FROM processing_state ORDER BY id DESC LIMIT 1")
    result = cur.fetchone()
    stats['last_processed_row'] = result[0] if result else 0
    
    # Detection counts by pattern
    cur.execute("""
        SELECT pattern_id, action_type, COUNT(*) as count
        FROM detections
        GROUP BY pattern_id, action_type
        ORDER BY pattern_id
    """)
    stats['detections'] = cur.fetchall()
    
    # Pending detections (not uploaded)
    cur.execute("SELECT COUNT(*) FROM detections WHERE uploaded_to_s3 = FALSE")
    stats['pending_detections'] = cur.fetchone()[0]
    
    # Recent detections
    cur.execute("""
        SELECT detection_time, pattern_id, action_type, customer_name, merchant_id
        FROM detections
        ORDER BY detection_time DESC
        LIMIT 10
    """)
    stats['recent_detections'] = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return stats

def print_dashboard():
    """Print monitoring dashboard"""
    print("\n" + "=" * 80)
    print(f"Transaction Processing System - Dashboard [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    print("=" * 80)
    
    try:
        stats = get_system_stats()
        
        print("\n📊 PROCESSING STATS")
        print(f"  Total Transactions Processed: {stats['total_transactions']:,}")
        print(f"  Unique Customers: {stats['unique_customers']:,}")
        print(f"  Unique Merchants: {stats['unique_merchants']:,}")
        print(f"  Last Processed Row: {stats['last_processed_row']:,}")
        
        print("\n🎯 DETECTION SUMMARY")
        if stats['detections']:
            for pattern_id, action_type, count in stats['detections']:
                print(f"  {pattern_id} ({action_type}): {count:,} detections")
        else:
            print("  No detections yet")
        
        print(f"\n⏳ Pending Uploads: {stats['pending_detections']:,} detections")
        
        print("\n📋 RECENT DETECTIONS (Last 10)")
        if stats['recent_detections']:
            for det_time, pattern_id, action_type, cust_name, merchant_id in stats['recent_detections']:
                time_str = det_time.strftime('%H:%M:%S')
                cust_display = cust_name[:20] if cust_name else "-"
                print(f"  [{time_str}] {pattern_id} | {action_type:12} | {cust_display:20} | {merchant_id}")
        else:
            print("  No detections yet")
        
        # S3 files count
        try:
            input_files = s3_handler.list_s3_transaction_files()
            print(f"\n☁️  S3 INPUT FILES: {len(input_files)} chunks uploaded")
        except Exception as e:
            print(f"\n☁️  S3 INPUT FILES: Unable to fetch ({str(e)[:50]})")
        
    except Exception as e:
        print(f"\n❌ Error fetching stats: {e}")
    
    print("\n" + "=" * 80)

def monitor_continuous(interval=5):
    """Continuously monitor the system"""
    print("Starting continuous monitoring (Ctrl+C to stop)...")
    
    try:
        while True:
            print_dashboard()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        monitor_continuous(interval)
    else:
        print_dashboard()
