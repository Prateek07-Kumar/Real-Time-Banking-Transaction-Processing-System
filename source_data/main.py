# main.py
"""
Main entry point for the transaction processing system
Runs both Mechanism X and Y concurrently
"""
import threading
import time
import database
from mechanism_x import MechanismX
from mechanism_y import MechanismY

def run_mechanism_x():
    """Run Mechanism X in a separate thread"""
    try:
        mechanism_x = MechanismX()
        mechanism_x.run()
    except Exception as e:
        print(f"Mechanism X failed: {e}")
        import traceback
        traceback.print_exc()

def run_mechanism_y():
    """Run Mechanism Y in a separate thread"""
    try:
        # Wait a bit for Mechanism X to start uploading files
        time.sleep(2)
        mechanism_y = MechanismY()
        mechanism_y.run()
    except Exception as e:
        print(f"Mechanism Y failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Initialize system and start both mechanisms"""
    print("=" * 60)
    print("Transaction Processing System Starting...")
    print("=" * 60)
    
    # Initialize database
    print("\nInitializing database...")
    database.init_database()
    
    # Create threads for both mechanisms
    thread_x = threading.Thread(target=run_mechanism_x, name="MechanismX")
    thread_y = threading.Thread(target=run_mechanism_y, name="MechanismY")
    
    # Start both threads
    print("\nStarting Mechanism X and Y concurrently...")
    thread_x.start()
    thread_y.start()
    
    # Wait for both to complete
    try:
        thread_x.join()
        thread_y.join()
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    
    print("\n" + "=" * 60)
    print("Transaction Processing System Stopped")
    print("=" * 60)

if __name__ == "__main__":
    main()
