# reset_system.py
"""
Reset the system to start fresh
WARNING: This will delete all data!
"""
import database
import s3_handler
import config

def confirm_reset():
    """Ask for confirmation before reset"""
    print("\n⚠️  WARNING: This will DELETE ALL DATA! ⚠️")
    print("  - All transactions in database")
    print("  - All detections in database")
    print("  - All processing state")
    print("  - Will NOT delete S3 files (do manually if needed)")
    
    response = input("\nType 'RESET' to confirm: ")
    return response == 'RESET'

def reset_database():
    """Reset all database tables"""
    conn = database.get_db_connection()
    cur = conn.cursor()
    
    print("\n🗑️  Dropping and recreating tables...")
    
    # Drop tables
    cur.execute("DROP TABLE IF EXISTS detections CASCADE")
    cur.execute("DROP TABLE IF EXISTS transactions CASCADE")
    cur.execute("DROP TABLE IF EXISTS customer_importance CASCADE")
    cur.execute("DROP TABLE IF EXISTS processing_state CASCADE")
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Recreate tables
    database.init_database()
    print("✅ Database reset complete")

def main():
    if confirm_reset():
        reset_database()
        print("\n✨ System reset successfully!")
        print("You can now restart the processing system.")
    else:
        print("\n❌ Reset cancelled.")

if __name__ == "__main__":
    main()