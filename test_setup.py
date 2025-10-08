# test_setup.py
"""
Test script to verify system setup
"""
import sys

def test_imports():
    """Test if all required packages are installed"""
    print("Testing imports...")
    try:
        import boto3
        import psycopg2
        import pandas
        import google.oauth2
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import pytz
        print("✅ All required packages imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_config():
    """Test if configuration is set"""
    print("\nTesting configuration...")
    try:
        import config
        
        checks = {
            'AWS_ACCESS_KEY': bool(config.AWS_ACCESS_KEY),
            'AWS_SECRET_KEY': bool(config.AWS_SECRET_KEY),
            'S3_BUCKET': bool(config.S3_BUCKET),
            'DB_HOST': bool(config.DB_HOST),
            'DB_NAME': bool(config.DB_NAME),
            'DB_USER': bool(config.DB_USER),
            'DB_PASSWORD': bool(config.DB_PASSWORD),
        }
        
        all_good = all(checks.values())
        
        for key, value in checks.items():
            status = "✅" if value else "❌"
            print(f"  {status} {key}: {'Set' if value else 'Not set'}")
        
        return all_good
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_database():
    """Test database connection"""
    print("\nTesting database connection...")
    try:
        import database
        conn = database.get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result[0] == 1:
            print("✅ Database connection successful")
            return True
        else:
            print("❌ Database query failed")
            return False
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False

def test_s3():
    """Test S3 connection"""
    print("\nTesting S3 connection...")
    try:
        import s3_handler
        s3_client = s3_handler.get_s3_client()
        
        # Try to list buckets
        response = s3_client.list_buckets()
        print(f"✅ S3 connection successful (found {len(response['Buckets'])} buckets)")
        
        # Check if our bucket exists
        import config
        buckets = [b['Name'] for b in response['Buckets']]
        if config.S3_BUCKET in buckets:
            print(f"✅ Bucket '{config.S3_BUCKET}' exists")
        else:
            print(f"⚠️  Bucket '{config.S3_BUCKET}' not found. Create it with:")
            print(f"   aws s3 mb s3://{config.S3_BUCKET}")
        
        return True
    except Exception as e:
        print(f"❌ S3 connection error: {e}")
        return False

def test_google_drive():
    """Test Google Drive setup"""
    print("\nTesting Google Drive setup...")
    try:
        import os
        import config
        
        if os.path.exists(config.CREDENTIALS_FILE):
            print(f"✅ Credentials file found: {config.CREDENTIALS_FILE}")
        else:
            print(f"❌ Credentials file not found: {config.CREDENTIALS_FILE}")
            print("   Download from Google Cloud Console")
            return False
        
        if os.path.exists(config.TOKEN_FILE):
            print(f"✅ Token file found: {config.TOKEN_FILE}")
        else:
            print(f"⚠️  Token file not found: {config.TOKEN_FILE}")
            print("   Will be created on first run (requires browser auth)")
        
        return True
    except Exception as e:
        print(f"❌ Google Drive setup error: {e}")
        return False

def main():
    print("=" * 60)
    print("Transaction Processing System - Setup Test")
    print("=" * 60)
    
    results = []
    
    results.append(("Imports", test_imports()))
    results.append(("Configuration", test_config()))
    results.append(("Database", test_database()))
    results.append(("S3", test_s3()))
    results.append(("Google Drive", test_google_drive()))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:10} | {name}")
    
    all_passed = all(r[1] for r in results)
    
    if all_passed:
        print("\n🎉 All tests passed! System is ready.")
        print("Run 'python main.py' to start processing.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Fix the issues above before running.")
        return 1

if __name__ == "__main__":
    sys.exit(main())