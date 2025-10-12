# 🎯 Step-by-Step Execution Guide

This guide will walk you through setting up and running the Transaction Processing System from scratch.

## ⏱️ Estimated Setup Time: 30-45 minutes

---

## Phase 1: Environment Preparation (10 minutes)

### Step 1.1: Install Python and PostgreSQL

**For Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv postgresql postgresql-contrib
```

**For macOS:**
```bash
brew install python postgresql
```

**For Windows:**
- Download Python 3.8+ from [python.org](https://www.python.org/downloads/)
- Download PostgreSQL from [postgresql.org](https://www.postgresql.org/download/windows/)

### Step 1.2: Verify Installations

```bash
python3 --version  # Should be 3.8+
psql --version     # Should be 12+
```

### Step 1.3: Create Project Directory

```bash
mkdir transaction-processor
cd transaction-processor
```

---

## Phase 2: Project Setup (10 minutes)

### Step 2.1: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 2.2: Create Project Files

Copy all the code files from the artifacts into your project directory:

1. `config.py`
2. `database.py`
3. `gdrive_handler.py`
4. `s3_handler.py`
5. `mechanism_x.py`
6. `mechanism_y.py`
7. `main.py`
8. `monitor.py`
9. `reset_system.py`
10. `test_setup.py`
11. `performance_test.py`
12. `requirements.txt`

### Step 2.3: Install Dependencies

```bash
pip install -r requirements.txt
```

**Expected output:**
```
Successfully installed boto3-1.26.0 psycopg2-binary-2.9.0 pandas-2.0.0 ...
```

---

## Phase 3: Database Configuration (5 minutes)

### Step 3.1: Start PostgreSQL

**Ubuntu/Debian:**
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**macOS:**
```bash
brew services start postgresql
```

### Step 3.2: Create Database and User

```bash
# Access PostgreSQL
sudo -u postgres psql

# In psql prompt, run:
CREATE DATABASE transaction_db;
CREATE USER txprocessor WITH PASSWORD 'secure_password_123';
GRANT ALL PRIVILEGES ON DATABASE transaction_db TO txprocessor;
\q
```

### Step 3.3: Test Connection

```bash
psql -U txprocessor -d transaction_db -h localhost
# Enter password when prompted
# If successful, type \q to exit
```

---

## Phase 4: AWS S3 Setup (5 minutes)

### Step 4.1: Install AWS CLI

```bash
# For Ubuntu/Debian
sudo apt install awscli

# For macOS
brew install awscli

# For Windows
# Download from https://aws.amazon.com/cli/
```

### Step 4.2: Configure AWS Credentials

```bash
aws configure
```

**Enter when prompted:**
```
AWS Access Key ID: YOUR_ACCESS_KEY
AWS Secret Access Key: YOUR_SECRET_KEY
Default region name: us-east-1
Default output format: json
```

### Step 4.3: Create S3 Bucket

```bash
# Create bucket
aws s3 mb s3://transaction-processing-bucket

# Verify creation
aws s3 ls | grep transaction-processing
```

---

## Phase 5: Google Drive API Setup (10 minutes)

### Step 5.1: Enable Google Drive API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click "New Project" → Name it "Transaction Processor"
3. Click "Create"
4. Go to "APIs & Services" → "Library"
5. Search for "Google Drive API"
6. Click "Enable"

### Step 5.2: Create OAuth Credentials

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "OAuth client ID"
3. Configure consent screen if prompted:
   - User Type: External
   - App name: Transaction Processor
   - User support email: your email
   - Developer contact: your email
   - Click "Save and Continue" through all steps
4. Back to "Create OAuth client ID":
   - Application type: Desktop app
   - Name: Transaction Processor Desktop
   - Click "Create"
5. Click "Download JSON"
6. Rename downloaded file to `credentials.json`
7. Move to project directory

### Step 5.3: Prepare Google Drive Data

1. Create a folder in your Google Drive
2. Upload your `transactions.csv` file
3. Upload your `CustomerImportance.csv` file
4. Click on the folder → Get shareable link
5. Extract folder ID from URL:
   ```
   https://drive.google.com/drive/folders/1qryhdlgNsmecWRy2haI8S3uC63wKk5X-
                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                           This is your folder ID
   ```

---

## Phase 6: Configuration (5 minutes)

### Step 6.1: Create .env File

Create a file named `.env` in project root:

```bash
nano .env  # or use any text editor
```

Add the following (replace with your actual values):

```env
# AWS Configuration
AWS_ACCESS_KEY_ID=your_actual_access_key
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
AWS_REGION=us-east-1
S3_BUCKET=transaction-processing-bucket

# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=transaction_db
DB_USER=txprocessor
DB_PASSWORD=secure_password_123

# Google Drive Configuration (already in config.py)
# Just verify GDRIVE_FOLDER_ID in config.py matches your folder
```

### Step 6.2: Update config.py

Open `config.py` and update the Google Drive folder ID:

```python
# Find this line:
GDRIVE_FOLDER_ID = '1qryhdlgNsmecWRy2haI8S3uC63wKk5X-'

# Replace with your actual folder ID:
GDRIVE_FOLDER_ID = 'your_actual_folder_id_here'
```

Save the file.

---

## Phase 7: Initialize System (5 minutes)

### Step 7.1: Run Setup Test

```bash
python test_setup.py
```

**Expected output:**
```
============================================================
Transaction Processing System - Setup Test
============================================================
Testing imports...
✅ All required packages imported successfully

Testing configuration...
  ✅ AWS_ACCESS_KEY: Set
  ✅ AWS_SECRET_KEY: Set
  ✅ S3_BUCKET: Set
  ✅ DB_HOST: Set
  ✅ DB_NAME: Set
  ✅ DB_USER: Set
  ✅ DB_PASSWORD: Set

Testing database connection...
✅ Database connection successful

Testing S3 connection...
✅ S3 connection successful (found X buckets)
✅ Bucket 'transaction-processing-bucket' exists

Testing Google Drive setup...
✅ Credentials file found: credentials.json
⚠️  Token file not found: token.json
   Will be created on first run (requires browser auth)

============================================================
SUMMARY
============================================================
✅ PASS    | Imports
✅ PASS    | Configuration
✅ PASS    | Database
✅ PASS    | S3
✅ PASS    | Google Drive

🎉 All tests passed! System is ready.
Run 'python main.py' to start processing.
```

### Step 7.2: Initialize Database Schema

```bash
python -c "import database; database.init_database()"
```

**Expected output:**
```
Database initialized successfully
```

### Step 7.3: Verify Database Tables

```bash
psql -U txprocessor -d transaction_db -h localhost
```

In psql prompt:
```sql
\dt
```

**Expected output:**
```
                List of relations
 Schema |        Name         | Type  |    Owner    
--------+---------------------+-------+-------------
 public | customer_importance | table | txprocessor
 public | detections          | table | txprocessor
 public | processing_state    | table | txprocessor
 public | transactions        | table | txprocessor
(4 rows)
```

Type `\q` to exit.

---

## Phase 8: First Run (5 minutes)

### Step 8.1: Start the System

```bash
python main.py
```

**On first run**, a browser window will open asking you to:
1. Sign in to your Google account
2. Allow "Transaction Processor" to access your Google Drive
3. Click "Allow"

The browser will show "The authentication flow has completed. You may close this window."

### Step 8.2: Monitor Initial Processing

You should see output like:

```
============================================================
Transaction Processing System Starting...
============================================================

Initializing database...
Database initialized successfully

Starting Mechanism X and Y concurrently...
Starting Mechanism X...
Loading initial data from Google Drive...
Loaded 150000 transactions
Loaded 5000 customer importance records
Starting Mechanism Y...
Processed chunk 1: rows 0 to 10000
Processing file: input/transactions/chunk_1_20241007_143022.csv
Inserted 10000 transactions into database
Pattern 1: Detected 0 UPGRADE cases
Pattern 2: Detected 5 CHILD cases
Pattern 3: Detected 0 DEI-NEEDED cases
Uploaded 5 detections to S3: output/detections/detections_20241007_143025_123456.csv
Processed chunk 2: rows 10000 to 20000
...
```

### Step 8.3: Open Another Terminal for Monitoring

While the system is running, open a new terminal:

```bash
cd transaction-processor
source venv/bin/activate
python monitor.py --continuous 5
```

This will show real-time statistics every 5 seconds.

---

## Phase 9: Verification (5 minutes)

### Step 9.1: Check Database Content

```bash
psql -U txprocessor -d transaction_db -h localhost
```

Run queries:
```sql
-- Total transactions
SELECT COUNT(*) FROM transactions;

-- Detection summary
SELECT pattern_id, action_type, COUNT(*) 
FROM detections 
GROUP BY pattern_id, action_type;

-- Recent detections
SELECT * FROM detections ORDER BY detection_time DESC LIMIT 10;
```

### Step 9.2: Check S3 Files

```bash
# List input chunks
aws s3 ls s3://transaction-processing-bucket/input/transactions/

# List detection outputs
aws s3 ls s3://transaction-processing-bucket/output/detections/

# Download and view a detection file
aws s3 cp s3://transaction-processing-bucket/output/detections/detections_20241007_143025_123456.csv .
cat detections_20241007_143025_123456.csv
```

### Step 9.3: Verify Detection File Format

```csv
YStartTime(IST),DetectionTime(IST),PatternId,ActionType,CustomerName,MerchantId
2024-10-07 14:30:00,2024-10-07 14:35:22,PatId1,UPGRADE,John Doe,M001
2024-10-07 14:30:00,2024-10-07 14:35:23,PatId2,CHILD,Jane Smith,M002
2024-10-07 14:30:00,2024-10-07 14:35:24,PatId3,DEI-NEEDED,,M003
```

---

## Phase 10: Common Operations

### Stop the System

Press `Ctrl + C` in the terminal where `main.py` is running.

### Restart the System

The system is resumable! It will continue from where it left off:

```bash
python main.py
```

### View Current Progress

```bash
python monitor.py
```

### Reset Everything (Start Fresh)

⚠️ **Warning: This deletes all data!**

```bash
python reset_system.py
# Type 'RESET' to confirm
```

### Run Performance Tests

```bash
python performance_test.py
```

### View Detailed Analytics

```bash
psql -U txprocessor -d transaction_db -h localhost -f query_examples.sql
```

---

## Phase 11: Production Deployment (Optional)

### Using Docker

1. **Build and Start:**
```bash
docker-compose up -d
```

2. **View Logs:**
```bash
docker-compose logs -f
```

3. **Stop:**
```bash
docker-compose down
```

### Using Make Commands

```bash
# Setup
make setup

# Run
make run

# Stop
make stop

# Clean up
make clean

# View logs
make logs
```

---

## Troubleshooting Guide

### Issue 1: "Module not found" Error

**Solution:**
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate      # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

### Issue 2: PostgreSQL Connection Refused

**Solution:**
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Start if not running
sudo systemctl start postgresql

# Check if listening on correct port
sudo netstat -plnt | grep 5432
```

### Issue 3: Google Drive Authentication Loop

**Solution:**
```bash
# Delete token and re-authenticate
rm token.json
python main.py
```

### Issue 4: S3 Permission Denied

**Solution:**
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://transaction-processing-bucket/

# If fails, check IAM permissions in AWS Console
```

### Issue 5: System Runs But No Detections

**Possible Causes:**
1. Not enough data processed yet (need >50K for Pattern 1)
2. Data doesn't match pattern criteria
3. Detections already exist (system prevents duplicates)

**Solution:**
```bash
# Check transaction count
psql -U txprocessor -d transaction_db -c "SELECT COUNT(*) FROM transactions;"

# Check existing detections
psql -U txprocessor -d transaction_db -c "SELECT * FROM detections;"

# Reset and reprocess if needed
python reset_system.py
```

---

## Performance Tuning

### For Faster Processing

Edit `config.py`:
```python
CHUNK_SIZE = 20000  # Increase from 10000 (uses more memory)
DETECTION_BATCH_SIZE = 100  # Increase from 50
```

### For Limited Memory

Edit `config.py`:
```python
CHUNK_SIZE = 5000   # Decrease from 10000
DETECTION_BATCH_SIZE = 25   # Decrease from 50
```

### Scale Horizontally

Run multiple Mechanism Y instances:
```bash
# Terminal 1: Mechanism X
python -c "from mechanism_x import MechanismX; MechanismX().run()"

# Terminal 2: Mechanism Y Instance 1
python -c "from mechanism_y import MechanismY; MechanismY().run()"

# Terminal 3: Mechanism Y Instance 2
python -c "from mechanism_y import MechanismY; MechanismY().run()"
```

---

## Expected System Behavior

### Normal Operation

✅ Mechanism X processes 10,000 records every 1 second  
✅ Mechanism Y picks up files within 1-2 seconds  
✅ Pattern detection runs immediately after ingestion  
✅ Detections uploaded in batches of 50  
✅ No duplicate detections created  
✅ System is resumable after interruption  

### Performance Metrics (Sample Data)

- **100K transactions**: ~10 seconds processing
- **500K transactions**: ~50 seconds processing  
- **1M transactions**: ~100 seconds processing
- **Detection latency**: <3 seconds per pattern
- **S3 upload**: <1 second per chunk

---

## Data Format Reference

### transactions.csv Format

```csv
TransactionId,CustomerId,CustomerName,Gender,MerchantId,TransactionType,TransactionAmount,TransactionDate
TX00001,C001,John Doe,Male,M001,Online,150.50,2024-01-01 10:30:00
TX00002,C002,Jane Smith,Female,M002,POS,22.75,2024-01-01 11:15:00
```

### CustomerImportance.csv Format

```csv
CustomerId,TransactionType,Weightage,Fraud
C001,Online,0.85,0
C001,POS,0.92,0
C002,Online,0.15,0
```

### Detection Output Format

```csv
YStartTime(IST),DetectionTime(IST),PatternId,ActionType,CustomerName,MerchantId
2024-10-07 14:30:00,2024-10-07 14:35:22,PatId1,UPGRADE,John Doe,M001
2024-10-07 14:30:00,2024-10-07 14:35:23,PatId2,CHILD,Jane Smith,M002
2024-10-07 14:30:00,2024-10-07 14:35:24,PatId3,DEI-NEEDED,,M003
```

---

## Success Checklist

Before considering the setup complete, verify:

- [ ] All dependencies installed
- [ ] PostgreSQL running and accessible
- [ ] Database tables created
- [ ] AWS credentials configured
- [ ] S3 bucket exists and accessible
- [ ] Google Drive API enabled
- [ ] credentials.json downloaded
- [ ] Data files uploaded to Google Drive
- [ ] test_setup.py passes all checks
- [ ] System runs without errors
- [ ] Transactions being processed
- [ ] Detections being generated
- [ ] Files appearing in S3
- [ ] Monitor dashboard shows stats

---

## Next Steps

Once the system is running successfully:

1. **Monitor Performance**: Use `monitor.py` to track progress
2. **Analyze Results**: Query database with `query_examples.sql`
3. **Optimize**: Adjust chunk sizes based on your data volume
4. **Scale**: Add more Mechanism Y instances if needed
5. **Production**: Deploy with Docker for reliability

---

## Support & Resources

- **Test Setup**: `python test_setup.py`
- **Monitor**: `python monitor.py`
- **Reset**: `python reset_system.py`
- **Performance**: `python performance_test.py`
- **Documentation**: See README.md
- **SQL Queries**: See query_examples.sql

---

**🎉 Congratulations! Your Transaction Processing System is now operational!**