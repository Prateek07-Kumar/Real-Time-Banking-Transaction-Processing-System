# performance_test.py
"""
Performance testing script
"""
import time
import database
import random
from datetime import datetime, timedelta

def generate_test_transactions(count=10000):
    """Generate random test transactions"""
    print(f"Generating {count:,} test transactions...")
    
    transactions = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(count):
        transaction = (
            f"TX{i:08d}",  # transaction_id
            f"C{random.randint(1, 1000):05d}",  # customer_id
            f"Customer_{random.randint(1, 1000)}",  # customer_name
            random.choice(['Male', 'Female']),  # gender
            f"M{random.randint(1, 100):03d}",  # merchant_id
            random.choice(['Online', 'POS', 'ATM']),  # transaction_type
            round(random.uniform(10, 1000), 2),  # amount
            start_date + timedelta(days=random.randint(0, 365))  # date
        )
        transactions.append(transaction)
    
    return transactions

def test_bulk_insert():
    """Test bulk insert performance"""
    print("\n" + "=" * 60)
    print("Testing Bulk Insert Performance")
    print("=" * 60)
    
    test_data = generate_test_transactions(10000)
    
    start = time.time()
    database.insert_transactions(test_data)
    elapsed = time.time() - start
    
    rate = len(test_data) / elapsed
    
    print(f"\n✅ Inserted {len(test_data):,} transactions")
    print(f"⏱️  Time: {elapsed:.2f} seconds")
    print(f"📊 Rate: {rate:.0f} transactions/second")

def test_pattern_detection():
    """Test pattern detection performance"""
    print("\n" + "=" * 60)
    print("Testing Pattern Detection Performance")
    print("=" * 60)
    
    from mechanism_y import MechanismY
    
    mechanism_y = MechanismY()
    mechanism_y.y_start_time = datetime.now()
    
    start = time.time()
    
    detections_1 = mechanism_y.detect_pattern_1()
    time_1 = time.time() - start
    
    start = time.time()
    detections_2 = mechanism_y.detect_pattern_2()
    time_2 = time.time() - start
    
    start = time.time()
    detections_3 = mechanism_y.detect_pattern_3()
    time_3 = time.time() - start
    
    print(f"\n✅ Pattern 1 (UPGRADE): {len(detections_1)} detections in {time_1:.2f}s")
    print(f"✅ Pattern 2 (CHILD): {len(detections_2)} detections in {time_2:.2f}s")
    print(f"✅ Pattern 3 (DEI-NEEDED): {len(detections_3)} detections in {time_3:.2f}s")
    
    total_time = time_1 + time_2 + time_3
    total_detections = len(detections_1) + len(detections_2) + len(detections_3)
    
    print(f"\n📊 Total: {total_detections} detections in {total_time:.2f}s")

def main():
    print("=" * 60)
    print("Performance Testing")
    print("=" * 60)
    
    print("\n⚠️  This will add test data to your database.")
    response = input("Continue? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    test_bulk_insert()
    test_pattern_detection()
    
    print("\n" + "=" * 60)
    print("Performance testing complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()