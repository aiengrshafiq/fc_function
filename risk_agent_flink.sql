--********************************************************************--
-- Author:         DataArchitectAdmin
-- Created Time:   2025-12-03 07:45:24
-- Description:    Risk agent V10
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

-- q. Sanctions (address-level sanctions status)
CREATE TEMPORARY TABLE dim_sanctions_address (
    chain               STRING,
    destination_address STRING,
    is_sanctioned       BOOLEAN,
    sanctions_status    STRING,
    last_checked_at     TIMESTAMP_LTZ(3),
    last_error          STRING,
    PRIMARY KEY (chain, destination_address) NOT ENFORCED
) WITH (
    'connector' = 'hologres',
    'dbname'    = 'onebullex_rt',
    'tablename' = 'rt.dim_sanctions_address',
    'username'  = 'BASIC$shafiq',
    'password'  = 'HOLOGRES@424',
    'endpoint'  = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'     = 'true',
    'enable_binlog_filter_push_down' = 'false'
);

-- r. Destination Age (wallet age in hours)
CREATE TEMPORARY TABLE dim_destination_age (
    chain                STRING,
    destination_address  STRING,
    destination_age_hours DOUBLE,
    age_status           STRING,
    first_seen_at        TIMESTAMP_LTZ(3),
    last_checked_at      TIMESTAMP_LTZ(3),
    last_error           STRING,
    PRIMARY KEY (chain, destination_address) NOT ENFORCED
) WITH (
    'connector' = 'hologres',
    'dbname'    = 'onebullex_rt',
    'tablename' = 'rt.dim_destination_age',
    'username'  = 'BASIC$shafiq',
    'password'  = 'HOLOGRES@424',
    'endpoint'  = 'hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'     = 'true',
    'enable_binlog_filter_push_down' = 'false'
);

-- s. User Whitelist (rt.risk_whitelist_user)
CREATE TEMPORARY TABLE dim_risk_whitelist_user (
    user_code   STRING,
    description STRING,
    created_at  TIMESTAMP_LTZ(3),
    expires_at  TIMESTAMP_LTZ(3),
    status      STRING,
    PRIMARY KEY (user_code) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_whitelist_user',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
);

-- t. Address Whitelist (rt.risk_whitelist_address)
CREATE TEMPORARY TABLE dim_risk_whitelist_address (
    destination_address STRING,
    chain               STRING,
    description         STRING,
    created_at          TIMESTAMP_LTZ(3),
    expires_at          TIMESTAMP_LTZ(3),
    status              STRING,
    PRIMARY KEY (destination_address) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_whitelist_address',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
);

-- u. User Blacklist (rt.risk_blacklist_user)
CREATE TEMPORARY TABLE dim_risk_blacklist_user (
    user_code   STRING,
    reason      STRING,
    created_at  TIMESTAMP_LTZ(3),
    expires_at  TIMESTAMP_LTZ(3),
    status      STRING,
    PRIMARY KEY (user_code) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_blacklist_user',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
);

-- v. Address Blacklist (rt.risk_blacklist_address)
CREATE TEMPORARY TABLE dim_risk_blacklist_address (
    destination_address STRING,
    chain               STRING,
    reason              STRING,
    created_at          TIMESTAMP_LTZ(3),
    expires_at          TIMESTAMP_LTZ(3),
    status              STRING,
    PRIMARY KEY (destination_address) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_blacklist_address',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
);

-- w. IP Blacklist (rt.risk_blacklist_ip)
CREATE TEMPORARY TABLE dim_risk_blacklist_ip (
    ip_address STRING,
    reason     STRING,
    created_at TIMESTAMP_LTZ(3),
    expires_at TIMESTAMP_LTZ(3),
    status     STRING,
    PRIMARY KEY (ip_address) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_blacklist_ip',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
);

-- x. Generic Greylist (rt.risk_greylist)
CREATE TEMPORARY TABLE dim_risk_greylist (
    entity_value STRING,
    entity_type  STRING,
    reason       STRING,
    created_at   TIMESTAMP_LTZ(3),
    expires_at   TIMESTAMP_LTZ(3),
    status       STRING,
    PRIMARY KEY (entity_value, entity_type) NOT ENFORCED
) WITH (
    'connector'='hologres',
    'dbname'='onebullex_rt',
    'tablename'='rt.risk_greylist',
    'username'='BASIC$shafiq',
    'password'='HOLOGRES@424',
    'endpoint'='hgpost-sg-u7g4iu5x2002-ap-northeast-1-vpc-st.hologres.aliyuncs.com:80',
    'async'='true',
    'enable_binlog_filter_push_down'='false'
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
    device_density INT,
    cluster_newness_ratio DOUBLE,
    is_new_device BOOLEAN,
    is_new_ip BOOLEAN,
    is_impossible_travel BOOLEAN,
    time_since_critical_event DOUBLE,
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
    days_since_whitelist_add DOUBLE,
    hours_since_fiat_deposit DOUBLE,
    arbitrage_flag DOUBLE,
    update_time TIMESTAMP_LTZ(3),
    destination_address STRING,
    withdrawal_deviation DOUBLE,
    rapid_cycling BOOLEAN,
    time_since_user_login INT,
    withdrawal_amount DOUBLE,
    sanctions_status STRING,
    age_status STRING,
    -- NEW: list flags
    user_whitelisted    BOOLEAN,
    address_whitelisted BOOLEAN,
    user_blacklisted    BOOLEAN,
    address_blacklisted BOOLEAN,
    ip_blacklisted      BOOLEAN,
    user_greylisted     BOOLEAN,
    address_greylisted  BOOLEAN,
    ip_greylisted       BOOLEAN,
    withdraw_currency STRING,

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
    -- 1. Keys
    CAST(w.user_code AS STRING) AS user_code,
    COALESCE(CAST(w.code AS STRING), CAST(w.id AS STRING)) AS txn_id,

    -- 2. Fan-out / Fan-in / IP density / Device density
    CAST(COALESCE(dfo.fan_out_count, 0) AS INT)      AS deposit_fan_out,
    CAST(COALESCE(f.fan_in_count, 0) AS INT)         AS withdrawal_fan_in,
    CAST(COALESCE(ips.distinct_users_24h, 1) AS INT) AS ip_density,
    CAST(COALESCE(dd.user_count, 1) AS INT)          AS device_density,

    -- 3. Cluster Newness + Device flags
    CAST(COALESCE(inew.cluster_newness_ratio, 0.0) AS DOUBLE) AS cluster_newness_ratio,
    COALESCE(d.is_new_device, FALSE) AS is_new_device,
    COALESCE(d.is_new_ip, FALSE)     AS is_new_ip,

    -- 4. Impossible travel + time_since_critical_event (placeholder)
    COALESCE(it.is_impossible_travel, FALSE) AS is_impossible_travel,
    CAST(NULL AS DOUBLE)                     AS time_since_critical_event,

    -- 5. Withdrawal ratio
    CASE 
        WHEN c.total_balance_sum IS NULL OR c.total_balance_sum <= 0 THEN 1.0 
        ELSE CAST(CAST(w.withdraw_amount AS DOUBLE) / c.total_balance_sum AS DOUBLE)
    END AS withdrawal_ratio,

    -- 6. Session risk score
    COALESCE(d.session_risk_score, 0) AS session_risk_score,

    -- 7. Sanctions feature
    COALESCE(sanc.is_sanctioned, FALSE) AS is_sanctioned,

    -- 8. KYC + source_risk_score (placeholders for future)
    CAST(NULL AS DOUBLE) AS kyc_limit_utilization,
    0                    AS source_risk_score,

    -- 9. Destination age (wallet age in hours)
    CAST(COALESCE(age.destination_age_hours, 0.0) AS INT) AS destination_age_hours,

    -- 10. Passthrough turnover (trade_vol / withdraw_amount)
    CASE 
        WHEN CAST(w.withdraw_amount AS DOUBLE) <= 0 THEN 0.0
        ELSE CAST(COALESCE(tv.trade_vol_24h, 0.0) / CAST(w.withdraw_amount AS DOUBLE) AS DOUBLE)
    END AS passthrough_turnover,

    -- 11. Structuring
    CAST(COALESCE(s.structuring_count, 0) AS INT) AS structuring_velocity,

    -- 12. Account maturity
    CAST((w.create_at - c.create_at) / 86400000 AS INT) AS account_maturity,

    -- 13. Round-number withdrawal
    CASE WHEN CAST(w.withdraw_amount AS DOUBLE) % 1 = 0 THEN TRUE ELSE FALSE END AS is_round_number,

    -- 14. Abnormal PnL placeholder
    CAST(NULL AS DOUBLE) AS abnormal_pnl,

    -- 15. Whitelist freshness placeholder
    CAST(NULL AS DOUBLE) AS days_since_whitelist_add,

    -- 16. Deposit cooldown (hours_since_fiat_deposit)
    CASE 
        WHEN ld.last_deposit_ts IS NULL THEN 99999.0
        ELSE CAST((w.create_at - ld.last_deposit_ts) / 3600000.0 AS DOUBLE)
    END AS hours_since_fiat_deposit,

    -- 17. Arbitrage flag
    CASE 
        WHEN m.avg_price IS NOT NULL AND m.avg_price > 0 
        THEN CAST( (CAST(w.exchange_rate AS DOUBLE) - m.avg_price) / m.avg_price AS DOUBLE)
        ELSE 0.0 
    END AS arbitrage_flag,

    -- 18. Update_time + destination
    CURRENT_TIMESTAMP           AS update_time,
    w.address                   AS destination_address,

    -- 19. Withdrawal deviation (Z-score over 90d stats)
    CASE 
        WHEN z.stddev_amount IS NULL OR z.stddev_amount = 0 THEN 0.0
        ELSE CAST(
            (CAST(w.withdraw_amount AS DOUBLE) - z.avg_amount) / z.stddev_amount
            AS DOUBLE
        )
    END AS withdrawal_deviation,

    -- 20. Rapid cycling: withdraw within 5 minutes of last streaming deposit
    CASE
        WHEN sd.last_deposit_ts IS NOT NULL
             AND w.create_at >= sd.last_deposit_ts
             AND (w.create_at - sd.last_deposit_ts) <= 300000
        THEN TRUE
        ELSE FALSE
    END AS rapid_cycling,

    -- 21. time_since_user_login (minutes)
    CASE
        WHEN ll.last_login_ts IS NULL THEN 999999
        WHEN w.create_at <= ll.last_login_ts THEN 999999
        ELSE CAST( (w.create_at - ll.last_login_ts) / 60000.0 AS INT)
    END AS time_since_user_login,

    -- 22. Raw withdrawal amount
    CAST(w.withdraw_amount AS DOUBLE) AS withdrawal_amount,

    -- 23. Status columns from dims (fallback to PENDING)
    COALESCE(sanc.sanctions_status, 'PENDING') AS sanctions_status,
    COALESCE(age.age_status,        'PENDING') AS age_status,

    -- 24. User whitelist flag
    CASE 
        WHEN uw.user_code IS NOT NULL
         AND uw.status = 'ACTIVE'
         AND (uw.expires_at IS NULL OR uw.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS user_whitelisted,

    -- 25. Address whitelist flag
    CASE 
        WHEN wa.destination_address IS NOT NULL
         AND wa.status = 'ACTIVE'
         AND (wa.expires_at IS NULL OR wa.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS address_whitelisted,

    -- 26. User blacklist flag
    CASE 
        WHEN bu.user_code IS NOT NULL
         AND bu.status = 'ACTIVE'
         AND (bu.expires_at IS NULL OR bu.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS user_blacklisted,

    -- 27. Address blacklist flag
    CASE 
        WHEN ba.destination_address IS NOT NULL
         AND ba.status = 'ACTIVE'
         AND (ba.expires_at IS NULL OR ba.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS address_blacklisted,

    -- 28. IP blacklist flag
    CASE 
        WHEN bi.ip_address IS NOT NULL
         AND bi.status = 'ACTIVE'
         AND (bi.expires_at IS NULL OR bi.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS ip_blacklisted,

    -- 29. User greylist flag
    CASE 
        WHEN gu.entity_value IS NOT NULL
         AND gu.status = 'ACTIVE'
         AND (gu.expires_at IS NULL OR gu.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS user_greylisted,

    -- 30. Address greylist flag
    CASE 
        WHEN ga.entity_value IS NOT NULL
         AND ga.status = 'ACTIVE'
         AND (ga.expires_at IS NULL OR ga.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS address_greylisted,

    -- 31. IP greylist flag
    CASE 
        WHEN gi.entity_value IS NOT NULL
         AND gi.status = 'ACTIVE'
         AND (gi.expires_at IS NULL OR gi.expires_at > CURRENT_TIMESTAMP)
        THEN TRUE ELSE FALSE
    END AS ip_greylisted,

    -- 32. Token / currency symbol
    w.withdraw_currency AS withdraw_currency



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
-- 9. Device Stats
LEFT JOIN dim_device_density_24h FOR SYSTEM_TIME AS OF w.proc_time AS dd
    ON d.last_device_id = dd.device_id
-- 10. Last Deposit (Cooldown)
LEFT JOIN dim_last_deposit_cache FOR SYSTEM_TIME AS OF w.proc_time AS ld
    ON w.user_code = ld.user_code
-- 11. Cluster Newness
LEFT JOIN dim_ip_newness_24h FOR SYSTEM_TIME AS OF w.proc_time AS inew
    ON d.last_ip = inew.ip
-- 12. Withdrawal stats 90d
LEFT JOIN dim_withdrawal_stats_90d FOR SYSTEM_TIME AS OF w.proc_time AS z
    ON w.user_code = z.user_code
-- 13. Streaming last trade (reserved)
LEFT JOIN dim_streaming_last_trade FOR SYSTEM_TIME AS OF w.proc_time AS st
    ON w.user_code = st.user_code
-- 14. Streaming last deposit (rapid_cycling)
LEFT JOIN dim_streaming_last_deposit FOR SYSTEM_TIME AS OF w.proc_time AS sd
    ON w.user_code = sd.user_code
-- 15. Last login (time_since_user_login)
LEFT JOIN dim_last_login_cache FOR SYSTEM_TIME AS OF w.proc_time AS ll
    ON w.user_code = ll.user_code
-- 16. Impossible travel
LEFT JOIN dim_impossible_travel FOR SYSTEM_TIME AS OF w.proc_time AS it
    ON w.user_code = it.user_code
-- 17. Sanctions dim
LEFT JOIN dim_sanctions_address FOR SYSTEM_TIME AS OF w.proc_time AS sanc
    ON w.withdraw_currency = sanc.chain
   AND w.address           = sanc.destination_address
-- 18. Destination age dim
LEFT JOIN dim_destination_age FOR SYSTEM_TIME AS OF w.proc_time AS age
    ON w.withdraw_currency = age.chain
   AND w.address           = age.destination_address

-- 19. User whitelist
LEFT JOIN dim_risk_whitelist_user FOR SYSTEM_TIME AS OF w.proc_time AS uw
    ON CAST(w.user_code AS STRING) = uw.user_code
-- 20. Address whitelist
LEFT JOIN dim_risk_whitelist_address FOR SYSTEM_TIME AS OF w.proc_time AS wa
    ON w.address = wa.destination_address
-- 21. User blacklist
LEFT JOIN dim_risk_blacklist_user FOR SYSTEM_TIME AS OF w.proc_time AS bu
    ON CAST(w.user_code AS STRING) = bu.user_code
-- 22. Address blacklist
LEFT JOIN dim_risk_blacklist_address FOR SYSTEM_TIME AS OF w.proc_time AS ba
    ON w.address = ba.destination_address
-- 23. IP blacklist
LEFT JOIN dim_risk_blacklist_ip FOR SYSTEM_TIME AS OF w.proc_time AS bi
    ON d.last_ip = bi.ip_address
-- 24. User greylist
LEFT JOIN dim_risk_greylist FOR SYSTEM_TIME AS OF w.proc_time AS gu
    ON gu.entity_type = 'USER_CODE'
   AND gu.entity_value = CAST(w.user_code AS STRING)
-- 25. Address greylist
LEFT JOIN dim_risk_greylist FOR SYSTEM_TIME AS OF w.proc_time AS ga
    ON ga.entity_type = 'DESTINATION_ADDRESS'
   AND ga.entity_value = w.address
-- 26. IP greylist
LEFT JOIN dim_risk_greylist FOR SYSTEM_TIME AS OF w.proc_time AS gi
    ON gi.entity_type = 'IP_ADDRESS'
   AND gi.entity_value = d.last_ip;
