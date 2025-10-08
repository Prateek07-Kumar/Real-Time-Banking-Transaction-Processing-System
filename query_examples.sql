-- query_examples.sql

-- SQL queries for manual analysis and debugging

-- ============================================
-- BASIC STATISTICS
-- ============================================

-- Total transactions by merchant
SELECT 
    merchant_id,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(transaction_amount) as total_amount,
    AVG(transaction_amount) as avg_amount
FROM transactions
GROUP BY merchant_id
ORDER BY total_transactions DESC
LIMIT 20;

-- Customer transaction summary
SELECT 
    customer_name,
    customer_id,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT merchant_id) as merchants_used,
    SUM(transaction_amount) as total_spent,
    AVG(transaction_amount) as avg_transaction
FROM transactions
GROUP BY customer_name, customer_id
ORDER BY total_transactions DESC
LIMIT 20;

-- ============================================
-- PATTERN 1 ANALYSIS (UPGRADE)
-- ============================================

-- Merchants eligible for Pattern 1 (>50K transactions)
SELECT 
    merchant_id,
    COUNT(*) as total_transactions
FROM transactions
GROUP BY merchant_id
HAVING COUNT(*) > 50000
ORDER BY total_transactions DESC;

-- Top customers by transaction count per merchant
WITH customer_counts AS (
    SELECT 
        merchant_id,
        customer_id,
        customer_name,
        COUNT(*) as tx_count
    FROM transactions
    WHERE merchant_id IN (
        SELECT merchant_id 
        FROM transactions 
        GROUP BY merchant_id 
        HAVING COUNT(*) > 50000
    )
    GROUP BY merchant_id, customer_id, customer_name
)
SELECT 
    merchant_id,
    customer_name,
    tx_count,
    PERCENT_RANK() OVER (PARTITION BY merchant_id ORDER BY tx_count) as percentile
FROM customer_counts
ORDER BY merchant_id, percentile DESC;

-- Customer weightage analysis
SELECT 
    t.customer_id,
    t.customer_name,
    t.merchant_id,
    COUNT(*) as total_tx,
    AVG(COALESCE(ci.weightage, 0)) as avg_weightage
FROM transactions t
LEFT JOIN customer_importance ci 
    ON t.customer_id = ci.customer_id 
    AND t.transaction_type = ci.transaction_type
GROUP BY t.customer_id, t.customer_name, t.merchant_id
ORDER BY avg_weightage ASC
LIMIT 20;

-- ============================================
-- PATTERN 2 ANALYSIS (CHILD)
-- ============================================

-- Customers with low average transaction amount
SELECT 
    customer_name,
    merchant_id,
    COUNT(*) as transaction_count,
    AVG(transaction_amount) as avg_amount,
    MIN(transaction_amount) as min_amount,
    MAX(transaction_amount) as max_amount
FROM transactions
GROUP BY customer_name, merchant_id
HAVING COUNT(*) >= 80 AND AVG(transaction_amount) < 23
ORDER BY avg_amount ASC;

-- Transaction amount distribution
SELECT 
    CASE 
        WHEN transaction_amount < 10 THEN '0-10'
        WHEN transaction_amount < 23 THEN '10-23'
        WHEN transaction_amount < 50 THEN '23-50'
        WHEN transaction_amount < 100 THEN '50-100'
        ELSE '100+'
    END as amount_range,
    COUNT(*) as count
FROM transactions
GROUP BY amount_range
ORDER BY amount_range;

-- ============================================
-- PATTERN 3 ANALYSIS (DEI-NEEDED)
-- ============================================

-- Gender distribution by merchant
SELECT 
    merchant_id,
    SUM(CASE WHEN UPPER(gender) = 'MALE' THEN 1 ELSE 0 END) as male_count,
    SUM(CASE WHEN UPPER(gender) = 'FEMALE' THEN 1 ELSE 0 END) as female_count,
    SUM(CASE WHEN UPPER(gender) NOT IN ('MALE', 'FEMALE') THEN 1 ELSE 0 END) as other_count,
    COUNT(DISTINCT customer_id) as total_unique_customers
FROM transactions
GROUP BY merchant_id
ORDER BY female_count DESC;

-- Merchants needing DEI focus
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
SELECT 
    merchant_id,
    female_count,
    male_count,
    male_count - female_count as gender_gap,
    ROUND(female_count::numeric / NULLIF(male_count, 0) * 100, 2) as female_ratio_pct
FROM gender_counts
WHERE female_count > 100
  AND male_count > female_count
ORDER BY gender_gap DESC;

-- ============================================
-- DETECTION TRACKING
-- ============================================

-- All detections summary
SELECT 
    pattern_id,
    action_type,
    COUNT(*) as total_detections,
    COUNT(DISTINCT customer_name) as unique_customers,
    COUNT(DISTINCT merchant_id) as unique_merchants,
    MIN(detection_time) as first_detection,
    MAX(detection_time) as last_detection
FROM detections
GROUP BY pattern_id, action_type
ORDER BY pattern_id;

-- Detection timeline (hourly)
SELECT 
    DATE_TRUNC('hour', detection_time) as hour,
    pattern_id,
    COUNT(*) as detections
FROM detections
GROUP BY hour, pattern_id
ORDER BY hour DESC, pattern_id;

-- Customers with multiple pattern detections
SELECT 
    customer_name,
    COUNT(DISTINCT pattern_id) as patterns_detected,
    STRING_AGG(DISTINCT pattern_id || '(' || action_type || ')', ', ') as patterns
FROM detections
WHERE customer_name != ''
GROUP BY customer_name
HAVING COUNT(DISTINCT pattern_id) > 1
ORDER BY patterns_detected DESC;

-- ============================================
-- PERFORMANCE MONITORING
-- ============================================

-- Processing state
SELECT 
    last_processed_row,
    updated_at,
    NOW() - updated_at as time_since_update
FROM processing_state
ORDER BY id DESC
LIMIT 1;

-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Recent database activity
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    query,
    NOW() - query_start as running_time
FROM pg_stat_activity
WHERE datname = 'transaction_db'
  AND pid != pg_backend_pid()
ORDER BY query_start DESC;

-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- Find duplicate transactions
SELECT 
    transaction_id,
    COUNT(*) as duplicate_count
FROM transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Transactions with missing data
SELECT 
    COUNT(*) FILTER (WHERE customer_id IS NULL OR customer_id = '') as missing_customer,
    COUNT(*) FILTER (WHERE merchant_id IS NULL OR merchant_id = '') as missing_merchant,
    COUNT(*) FILTER (WHERE transaction_amount IS NULL OR transaction_amount = 0) as missing_amount,
    COUNT(*) FILTER (WHERE transaction_date IS NULL) as missing_date,
    COUNT(*) as total_transactions
FROM transactions;

-- Gender data quality
SELECT 
    UPPER(gender) as gender_normalized,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM transactions
GROUP BY UPPER(gender)
ORDER BY count DESC;

-- Transaction amount outliers
SELECT 
    transaction_id,
    customer_name,
    merchant_id,
    transaction_amount,
    transaction_type
FROM transactions
WHERE transaction_amount > (
    SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount)
    FROM transactions
)
ORDER BY transaction_amount DESC
LIMIT 20;