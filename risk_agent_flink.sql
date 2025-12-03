--********************************************************************--
-- Author:         DataArchitectAdmin
-- Created Time:   2025-11-26 07:45:24
-- Description:    Risk agent v9
-- Hints:          You can use SET statements to modify the configuration
--********************************************************************--

-- 1. Flink Job Configuration
SET 'execution.checkpointing.interval' = '60s';
SET 'state.backend' = 'rocksdb'; 
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';

-- =========================================
-- 2. SOURCE: Withdrawal Records
-- =========================================
CREATE TEMPORARY TABLE source_withdraw_record (
    id BIGINT,
    code BIGINT,            --  ADDED: Required for txn_id
    user_code BIGINT,       
    create_at BIGINT,      
    withdraw_amount STRING, 
    address STRING,
    withdraw_currency STRING,
    exchange_rate STRING,
    proc_time AS PROCTIME(),
    event_time AS TO_TIMESTAMP_LTZ(create_at, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'onebullex.cdc.withdraw_record',
    'properties.bootstrap.servers' = 'alikafka-post-public-intl-sg-jiy4iu49v0b-1-vpc.alikafka.aliyuncs.com:9092,alikafka-post-public-intl-sg-jiy4iu49v0b-2-vpc.alikafka.aliyuncs.com:9092,alikafka-post-public-intl-sg-jiy4iu49v0b-3-vpc.alikafka.aliyuncs.com:9092',
    'properties.group.id' = 'obx-risk-phase5-final-v1', 
    'scan.startup.mode' = 'earliest-offset', 
    'format' = 'canal-json',
    'canal-json.ignore-parse-errors' = 'true'
);

-- =========================================
-- 3. LOOKUPS (Total 9 Tables now!)
-- =========================================

-- A. User Profile (Balance)
CREATE TEMPORARY TABLE dim_risk_lookup (
    user_code BIGINT, create_at BIGINT, google_auth_code STRING, total_balance_sum DOUBLE, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_user_risk_cache', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- B. Device Profile (Session Score + Last Device ID)
CREATE TEMPORARY TABLE dim_device_risk_cache (
    user_code BIGINT, 
    last_ip STRING, 
    last_device_id STRING, 
    session_risk_score INT, 
    is_root BOOLEAN,
    is_emulator BOOLEAN,
    is_new_device BOOLEAN,
    is_new_ip BOOLEAN,
    
    PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_device_risk_cache', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- C. Structuring
CREATE TEMPORARY TABLE dim_structuring_24h (
    user_code BIGINT, structuring_count INT, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_structuring_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- D. Fan-In
CREATE TEMPORARY TABLE dim_fan_in_24h (
    destination_address STRING, fan_in_count INT, PRIMARY KEY (destination_address) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_fan_in_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- E. Fan-Out
CREATE TEMPORARY TABLE dim_deposit_fan_out_cache (
    source_address STRING, fan_out_count INT, PRIMARY KEY (source_address) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_deposit_fan_out_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- F. Market Prices
CREATE TEMPORARY TABLE dim_market_prices (
    symbol STRING, avg_price DOUBLE, PRIMARY KEY (symbol) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.market_index_prices', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- G. Trade Volume
CREATE TEMPORARY TABLE dim_trade_volume_24h (
    user_code BIGINT, trade_vol_24h DOUBLE, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_trade_volume_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- H. IP Stats
CREATE TEMPORARY TABLE dim_ip_stats_24h (
    ip STRING, distinct_users_24h INT, PRIMARY KEY (ip) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_ip_stats_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- I. Device Stats ( NEW TABLE)
CREATE TEMPORARY TABLE dim_device_density_24h (
    device_id STRING, user_count INT, PRIMARY KEY (device_id) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_device_density_24h', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- J. Last Deposit (NEW TABLE)
CREATE TEMPORARY TABLE dim_last_deposit_cache (
    user_code BIGINT, last_deposit_ts BIGINT, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_last_deposit_cache', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- k. Cluster Newness
CREATE TEMPORARY TABLE dim_ip_newness_24h (
    ip STRING, cluster_newness_ratio DOUBLE, PRIMARY KEY (ip) NOT ENFORCED
) WITH (
    'connector' = 'hologres', 'dbname' = 'onebullex_rt', 'tablename' = 'rt.dim_ip_newness_24h', 
    'username' = 'BASIC$shafiq', 'password' = 'HOLOGRES@424', 'endpoint' = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async' = 'true', 'enable_binlog_filter_push_down' = 'false'
);

-- l. Withdrawal Stats 90d ( NEW!)

CREATE TEMPORARY TABLE dim_withdrawal_stats_90d (
    user_code BIGINT, avg_amount DOUBLE, stddev_amount DOUBLE, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_withdrawal_stats_90d', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');


-- m. Streaming Last Trade ( NEW for Rapid Cycling)
CREATE TEMPORARY TABLE dim_streaming_last_trade (
    user_code BIGINT, last_trade_ts BIGINT, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_streaming_last_trade', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- n. Streaming Last Deposit ( NEW for Rapid Cycling)
CREATE TEMPORARY TABLE dim_streaming_last_deposit (
    user_code BIGINT, last_deposit_ts BIGINT, PRIMARY KEY (user_code) NOT ENFORCED
) WITH ('connector'='hologres', 'dbname'='onebullex_rt', 'tablename'='rt.dim_streaming_last_deposit', 'username'='BASIC$shafiq', 'password'='HOLOGRES@424', 'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80', 'async'='true', 'enable_binlog_filter_push_down'='false');

-- o. Last Login Cache (for time_since_user_login)
CREATE TEMPORARY TABLE dim_last_login_cache (
    user_code     BIGINT,
    last_login_ts BIGINT,
    PRIMARY KEY (user_code) NOT ENFORCED
) WITH (
    'connector' = 'hologres',
    'dbname'    = 'onebullex_rt',
    'tablename' = 'rt.dim_last_login_cache',
    'username'  = 'BASIC$shafiq',
    'password'  = 'HOLOGRES@424',
    'endpoint'  = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'     = 'true',
    'enable_binlog_filter_push_down' = 'false'
);

-- p. Impossibe Travel
CREATE TEMPORARY TABLE dim_impossible_travel (
    user_code            BIGINT,
    is_impossible_travel BOOLEAN,
    last_withdraw_event  BIGINT,
    PRIMARY KEY (user_code) NOT ENFORCED
) WITH (
    'connector' = 'hologres',
    'dbname'    = 'onebullex_rt',
    'tablename' = 'rt.dim_impossible_travel',
    'username'  = 'BASIC$shafiq',
    'password'  = 'HOLOGRES@424',
    'endpoint'  = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'     = 'true',
    'enable_binlog_filter_push_down' = 'false'
);



-- =========================================
-- 4. SINK: Wide Table
-- =========================================
CREATE TEMPORARY TABLE risk_sink (
    user_code STRING,
    txn_id STRING,
    
    deposit_fan_out INT,
    withdrawal_fan_in INT,
    ip_density INT,
    
    device_density INT, --  Populated Now
    
    cluster_newness_ratio DOUBLE,
    is_new_device BOOLEAN,
    is_new_ip BOOLEAN,
    is_impossible_travel BOOLEAN,
    time_since_critical_event DOUBLE,
    time_since_user_login INT,
    withdrawal_ratio DOUBLE,
    session_risk_score INT,
    is_sanctioned BOOLEAN,
    kyc_limit_utilization DOUBLE,
    source_risk_score INT,
    destination_age_hours INT,
    passthrough_turnover DOUBLE,
    structuring_velocity INT,
    account_maturity INT,
    is_round_number BOOLEAN,
    abnormal_pnl DOUBLE,
    withdrawal_deviation DOUBLE,
    rapid_cycling BOOLEAN,
    days_since_whitelist_add DOUBLE,
    
    hours_since_fiat_deposit DOUBLE, --  Populated Now (as Deposit Cooldown)
    
    arbitrage_flag DOUBLE,
    update_time TIMESTAMP_LTZ(3),
    destination_address STRING,
    withdrawal_amount DOUBLE,
    PRIMARY KEY (user_code, txn_id) NOT ENFORCED
) WITH (
    'connector' = 'hologres',
    'dbname' = 'onebullex_rt',
    'tablename' = 'rt.risk_features',
    'mutatetype' = 'insertorupdate',
    'username' = 'BASIC$shafiq',
    'password' = 'HOLOGRES@424',
    'endpoint' = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'sink.insert.batch-size' = '500',
    'sink.insert.flush-interval-ms' = '1000'
);

-- =========================================
-- 5. LOGIC: Join All 9 Lookups
-- =========================================
INSERT INTO risk_sink
SELECT
    CAST(w.user_code AS STRING),
    --  FIX: Using 'w.code'
    COALESCE(CAST(w.code AS STRING), CAST(w.id AS STRING)),

    CAST(COALESCE(dfo.fan_out_count, 0) AS INT), -- Deposit Fan-Out
    CAST(COALESCE(f.fan_in_count, 0) AS INT),    -- Withdrawal Fan-In
    CAST(COALESCE(ips.distinct_users_24h, 1) AS INT), -- IP Density

    -- ✅ Device Density (NEW)
    -- Logic: Join Device Cache (to get Device ID) -> Join Device Stats
    CAST(COALESCE(dd.user_count, 1) AS INT) AS device_density,

    CAST(COALESCE(inew.cluster_newness_ratio, 0.0) AS DOUBLE) AS cluster_newness_ratio,

    
    COALESCE(d.is_new_device, FALSE), -- Default to False (Safe) if cache missing
    COALESCE(d.is_new_ip, FALSE),
    
    COALESCE(it.is_impossible_travel, FALSE) AS is_impossible_travel, 
    CAST(NULL AS DOUBLE),
    CASE                             -- 🔹 time_since_user_login (minutes)
        WHEN ll.last_login_ts IS NULL THEN 999999
        WHEN w.create_at <= ll.last_login_ts THEN 999999
        ELSE CAST( (w.create_at - ll.last_login_ts) / 60000.0 AS INT)
    END AS time_since_user_login,

    -- Ratio
    CASE 
        WHEN c.total_balance_sum IS NULL OR c.total_balance_sum <= 0 THEN 1.0 
        ELSE CAST(CAST(w.withdraw_amount AS DOUBLE) / c.total_balance_sum AS DOUBLE)
    END,

    COALESCE(d.session_risk_score, 0),

    FALSE, CAST(NULL AS DOUBLE), 0, 0, 

    -- Turnover
    CASE 
        WHEN CAST(w.withdraw_amount AS DOUBLE) <= 0 THEN 0.0
        ELSE CAST(COALESCE(tv.trade_vol_24h, 0.0) / CAST(w.withdraw_amount AS DOUBLE) AS DOUBLE)
    END,

    CAST(COALESCE(s.structuring_count, 0) AS INT), -- Structuring
    CAST((w.create_at - c.create_at) / 86400000 AS INT), -- Maturity
    CASE WHEN CAST(w.withdraw_amount AS DOUBLE) % 1 = 0 THEN TRUE ELSE FALSE END, -- Round

    CAST(NULL AS DOUBLE), 
    -- ✅ Z-Score Anomaly (Mapped to 'abnormal_pnl' or similar slot, usually 'withdrawal_deviation' should be added to table if possible, but here using placeholder slot if needed. Assuming abnormal_pnl is appropriate or you can rename column in Sink)
    -- Logic: (Current - Avg) / StdDev
    CASE 
        WHEN z.stddev_amount IS NULL OR z.stddev_amount = 0 THEN 0.0
        ELSE CAST( (CAST(w.withdraw_amount AS DOUBLE) - z.avg_amount) / z.stddev_amount AS DOUBLE)
    END,
    --  Rapid Cycling Logic (Check against Streaming Caches)
    -- 300,000 ms = 5 minutes
    -- CASE 
    --     WHEN (w.create_at - COALESCE(st.last_trade_ts, 0)) < 300000 THEN TRUE
    --     WHEN (w.create_at - COALESCE(sd.last_deposit_ts, 0)) < 300000 THEN TRUE
    --     ELSE FALSE
    -- END,
    CASE
        WHEN sd.last_deposit_ts IS NOT NULL
            AND w.create_at >= sd.last_deposit_ts
            AND (w.create_at - sd.last_deposit_ts) <= 300000
        THEN TRUE      -- any withdraw within 5 mins of deposit
        ELSE FALSE
    END AS rapid_cycling,
    
    CAST(NULL AS DOUBLE), -- Whitelist

    -- ✅ Deposit Cooldown (NEW)
    -- Logic: (Current Time - Last Deposit Time) in Hours
    -- Handle NULL last_deposit (Never deposited) -> Return -1 or huge number (safe)
    CASE 
        WHEN ld.last_deposit_ts IS NULL THEN 99999.0 -- No deposits = Long time ago
        ELSE CAST((w.create_at - ld.last_deposit_ts) / 3600000.0 AS DOUBLE)
    END AS hours_since_fiat_deposit,

    --CAST(NULL AS DOUBLE), this is extra
    
    -- Arbitrage
    CASE 
        WHEN m.avg_price IS NOT NULL AND m.avg_price > 0 
        THEN CAST( (CAST(w.exchange_rate AS DOUBLE) - m.avg_price) / m.avg_price AS DOUBLE)
        ELSE 0.0 
    END,

    CURRENT_TIMESTAMP,
    w.address,
    CAST(w.withdraw_amount AS DOUBLE)
    

FROM source_withdraw_record AS w
-- 1. User Profile
JOIN dim_risk_lookup FOR SYSTEM_TIME AS OF w.proc_time AS c
    ON w.user_code = c.user_code
-- 2. Device Profile
LEFT JOIN dim_device_risk_cache FOR SYSTEM_TIME AS OF w.proc_time AS d
    ON w.user_code = d.user_code
-- 3. Structuring
LEFT JOIN dim_structuring_24h FOR SYSTEM_TIME AS OF w.proc_time AS s
    ON w.user_code = s.user_code
-- 4. Fan-In
LEFT JOIN dim_fan_in_24h FOR SYSTEM_TIME AS OF w.proc_time AS f
    ON w.address = f.destination_address
-- 5. Fan-Out
LEFT JOIN dim_deposit_fan_out_cache FOR SYSTEM_TIME AS OF w.proc_time AS dfo
    ON w.address = dfo.source_address
-- 6. Market Price
LEFT JOIN dim_market_prices FOR SYSTEM_TIME AS OF w.proc_time AS m
    ON w.withdraw_currency = m.symbol
-- 7. Trade Volume
LEFT JOIN dim_trade_volume_24h FOR SYSTEM_TIME AS OF w.proc_time AS tv
    ON w.user_code = tv.user_code
-- 8. IP Stats
LEFT JOIN dim_ip_stats_24h FOR SYSTEM_TIME AS OF w.proc_time AS ips
    ON d.last_ip = ips.ip
-- 9. Device Stats (NEW JOIN)
LEFT JOIN dim_device_density_24h FOR SYSTEM_TIME AS OF w.proc_time AS dd
    ON d.last_device_id = dd.device_id
-- 10. Last Deposit (NEW JOIN)
LEFT JOIN dim_last_deposit_cache FOR SYSTEM_TIME AS OF w.proc_time AS ld
    ON w.user_code = ld.user_code
-- 11. newness
LEFT JOIN dim_ip_newness_24h FOR SYSTEM_TIME AS OF w.proc_time AS inew
    ON d.last_ip = inew.ip

-- 12 NEW JOIN for Z-Score
LEFT JOIN dim_withdrawal_stats_90d FOR SYSTEM_TIME AS OF w.proc_time AS z ON w.user_code = z.user_code

-- 13 NEW JOIN for rapid_cycling
LEFT JOIN dim_streaming_last_trade FOR SYSTEM_TIME AS OF w.proc_time AS st ON w.user_code = st.user_code
-- 14 NEW JOIN for rapid_cycling
LEFT JOIN dim_streaming_last_deposit FOR SYSTEM_TIME AS OF w.proc_time AS sd ON w.user_code = sd.user_code
-- 15 NEW JOIN for time_since_user_login
LEFT JOIN dim_last_login_cache FOR SYSTEM_TIME AS OF w.proc_time AS ll
    ON w.user_code = ll.user_code
-- 16 Impossible Travel
LEFT JOIN dim_impossible_travel FOR SYSTEM_TIME AS OF w.proc_time AS it
    ON w.user_code = it.user_code
