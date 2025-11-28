# -*- coding: utf-8 -*-
import json
import os
import base64
import time
import psycopg2
import urllib.request
import urllib.error
from urllib.parse import parse_qs
from datetime import datetime, timezone

print("[RISK_FC] System initializing - Final Production with Rule Engine v2.0")

# ==========================================
# 1. CONFIGURATION
# ==========================================
# DB Config (use env vars in production)
DB_HOST = os.environ.get("DB_HOST", "YOUR_HOLOGRES_HOST")
DB_PORT = int(os.environ.get("DB_PORT", "80"))
DB_NAME = os.environ.get("DB_NAME", "onebullex_rt")
DB_USER = os.environ.get("DB_USER", "YOUR_DB_USER")
DB_PASS = os.environ.get("DB_PASS", "YOUR_DB_PASSWORD")

# AI Config (Gemini)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# Chainalysis Config
CHAINALYSIS_API_KEY = os.environ.get("CHAINALYSIS_API_KEY", "")
CHAINALYSIS_URL = "https://public.chainalysis.com/api/v1/address"

# Lark Config
LARK_WEBHOOK_URL = os.environ.get(
    "LARK_WEBHOOK_URL",
    "https://open.larksuite.com/open-apis/bot/v2/hook/REPLACE_ME",
)

# Blockchair Config (Destination Age)
BLOCKCHAIR_API_KEY = os.environ.get("BLOCKCHAIR_API_KEY", "")
BLOCKCHAIR_BASE_URL = "https://api.blockchair.com"
DEST_AGE_CACHE_TTL = 3600 * 6  # 6 hours cache for same address
_DEST_AGE_CACHE = {}

# Caching
RULE_CACHE_TTL = 300
_RULES_CACHE = None
_LAST_CACHE_TIME = 0
_SANCTIONS_CACHE = {}
SANCTIONS_CACHE_TTL = 3600

# ==========================================
# 2. PROMPT
# ==========================================
COMPREHENSIVE_REASONING_PROMPT = """
You are the Senior Risk Officer for OneBullEx. The user has PASSED the hard validation rules (the obvious "Black/White" checks).
Your job is to detect **SUBTLE ANOMALIES** and **NON-HUMAN PATTERNS** in the "Gray Area".

**1. Feature Interpretation Guide (Contextual, not Mechanical):**
You will receive a JSON object containing ALL available risk features. 
* **Do not limit yourself to specific fields.** Use ANY data point in the JSON that helps form a risk narrative.
* **Infer the meaning** of features based on their names (e.g., if you see `mouse_movement_jitter` in the future, use it to judge intent).

**2. Assessment Pillars (Evaluate the INTENT):**

* **Pillar A: Anomalous Access (Is this the real user?)**
    * *Goal:* Detect subtle ATO signals.
    * *Reasoning:* Look for **consistency breaks**. Even if IP is not "New", is the *combination* of Device + Time + Location logical? Does the session look hurried (Account maturity vs current behavior)?

* **Pillar B: Illicit Flow (Is this money laundering?)**
    * *Goal:* Detect Mule/Layering activity.
    * *Reasoning:* Look at the **velocity and direction** of funds. Is the user acting as a "pass-through" node? Is the deposit source obscure while the destination is a fresh wallet? 

* **Pillar C: Integrity & Exploitation (Is this a scam/hack?)**
    * *Goal:* Detect manipulation.
    * *Reasoning:* Does the transaction make financial sense? Or does it look like a script exploiting a pricing bug, arbitrage, or a scam victim following instructions (round numbers)?

**3. Final Decision Logic (The "One-Strike" Rule):**
* **Score each Pillar (0-100)** based on the *intensity* of the anomaly.
* **MAX Score Strategy**: Your final `risk_score` is the HIGHEST score among the 3 pillars.
* **Threshold**: 
    * **HOLD (Score >= 75)**: If meaningful suspicion exists in ANY pillar.
    * **PASS (Score < 75)**: If behavior looks organic and human.

**4. Output Format:**
Return a single JSON object:
{
  "decision": "PASS" | "HOLD",
  "risk_score": 0-100,
  "primary_threat": "ATO" | "AML" | "FRAUD" | "NONE",
  "narrative": "Synthesize the 'Story'. Don't just list values."
}

**User Features (JSON):**
"""

# ==========================================
# 3. DATABASE HELPERS
# ==========================================
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS
    )


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col.name] = row[idx]
    return d


# --- LARK NOTIFICATION HELPER ---
def send_lark_notification(data):
    """
    Sends a rich card message to Lark.
    Only intended for REJECT/HOLD decisions.
    Fail-safe: any error is logged & ignored.
    """
    if not LARK_WEBHOOK_URL:
        return

    decision = data.get("decision", "HOLD")
    if decision not in ("REJECT", "HOLD"):
        # DON’T notify for PASS
        return

    try:
        color = "green" if decision == "PASS" else "red"
        title_emoji = "✅" if decision == "PASS" else "🚨"

        reasons = data.get("reasons", [])
        reason_text = (
            reasons[0]
            if isinstance(reasons, list) and reasons
            else str(reasons or data.get("narrative", "No details provided"))
        )

        card_content = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"{title_emoji} Risk Decision: {decision}",
                    },
                    "template": color,
                },
                "elements": [
                    {
                        "tag": "div",
                        "fields": [
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**User:**\n{data.get('user_code')}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**Txn ID:**\n{data.get('txn_id')}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**Threat:**\n{data.get('primary_threat')}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**Score:**\n{data.get('risk_score')}",
                                },
                            },
                        ],
                    },
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**Reasoning:**\n{reason_text}",
                        },
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"Source: {data.get('source')}",
                            }
                        ],
                    },
                ],
            },
        }

        req = urllib.request.Request(
            LARK_WEBHOOK_URL,
            data=json.dumps(card_content).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=2) as response:
            if response.status == 200:
                print("[RISK_FC] Lark notification sent.")
    except Exception as e:
        print(f"[RISK_FC] Lark Notification Error (Ignored): {e}")


# ------------------------------------------
# risk_features fetch & waits
# ------------------------------------------
def fetch_risk_features(user_code, txn_id):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = "SELECT * FROM rt.risk_features WHERE user_code = %s AND txn_id = %s"
        cur.execute(sql, (str(user_code), str(txn_id)))
        row = cur.fetchone()
        if row:
            return dict_factory(cur, row)
        return None
    except Exception as exc:
        print(f"[RISK_FC] Error fetching features: {exc}")
        return None
    finally:
        if conn:
            conn.close()


def wait_for_risk_features(user_code, txn_id, max_retries=5, delay=1.0):
    """
    Waits for rt.risk_features to be populated for (user_code, txn_id).
    Retries a few times to avoid race conditions with Flink.
    """
    for attempt in range(max_retries):
        features = fetch_risk_features(user_code, txn_id)
        if features:
            if attempt > 0:
                print(
                    f"[RISK_FC] risk_features found on attempt {attempt+1}/{max_retries}"
                )
            return features

        print(
            f"[RISK_FC] risk_features NOT found yet for user_code={user_code}, txn_id={txn_id} "
            f"(attempt {attempt+1}/{max_retries}), sleeping {delay}s"
        )
        time.sleep(delay)

    print(
        f"[RISK_FC] risk_features still missing after {max_retries} attempts for "
        f"user_code={user_code}, txn_id={txn_id}"
    )
    return None


# ------------------------------------------
# impossible travel & login timing helpers
# ------------------------------------------
def compute_impossible_travel(user_code):
    """
    Returns True if latest withdraw vs previous event looks like impossible travel,
    based on country change + short time + non-VPN.
    """
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT country_code, is_vpn, event_time
            FROM rt.user_device
            WHERE user_code = %s
              AND delete_at = 0
              AND operation = 'withdraw'
            ORDER BY event_time DESC
            LIMIT 1
        """,
            (str(user_code),),
        )
        latest = cur.fetchone()
        if not latest:
            print(f"[RISK_FC] No withdraw event in user_device for user {user_code}")
            return False

        last_country, last_is_vpn, last_ts = latest

        if last_country is None or last_ts is None:
            return False

        cur.execute(
            """
            SELECT country_code, is_vpn, event_time
            FROM rt.user_device
            WHERE user_code = %s
              AND delete_at = 0
              AND event_time < %s
            ORDER BY event_time DESC
            LIMIT 1
        """,
            (str(user_code), int(last_ts)),
        )
        prev = cur.fetchone()
        if not prev:
            print(
                f"[RISK_FC] No previous event before withdraw for user {user_code}"
            )
            return False

        prev_country, prev_is_vpn, prev_ts = prev
        if prev_country is None or prev_ts is None:
            return False

        if (prev_is_vpn == 1) or (last_is_vpn == 1):
            return False

        dt_ms = float(last_ts) - float(prev_ts)
        if dt_ms <= 0:
            return False
        dt_hours = dt_ms / 3600000.0

        if prev_country != last_country and dt_hours < 1.0:
            print(
                f"[RISK_FC] Impossible travel detected: {prev_country}->{last_country} in {dt_hours:.2f}h"
            )
            return True

        return False

    except Exception as e:
        print(f"[RISK_FC] Error computing impossible travel: {e}")
        return False
    finally:
        if conn:
            conn.close()


def update_impossible_travel_flag(user_code, txn_id, flag):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            UPDATE rt.risk_features
            SET is_impossible_travel = %s
            WHERE user_code = %s AND txn_id = %s
        """
        cur.execute(sql, (flag, str(user_code), str(txn_id)))
        conn.commit()
        print(
            f"[RISK_FC] Updated is_impossible_travel={flag} for user={user_code}, txn={txn_id}"
        )
    except Exception as e:
        print(f"[RISK_FC] Failed to update impossible travel flag: {e}")
    finally:
        if conn:
            conn.close()


def get_withdraw_timestamp_ms(user_code, txn_id):
    """
    Look up the withdraw timestamp (create_at in ms) for this (user_code, txn_id).
    txn_id in risk_features is coming from withdraw_record.code (string).
    """
    if not txn_id:
        return None

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            SELECT create_at
            FROM rt.withdraw_record
            WHERE user_code = %s
              AND code = %s
            ORDER BY create_at DESC
            LIMIT 1
        """
        try:
            code_val = int(str(txn_id))
        except Exception:
            code_val = str(txn_id)

        cur.execute(sql, (str(user_code), code_val))
        row = cur.fetchone()
        if row and row[0]:
            return int(row[0])
        return None
    except Exception as e:
        print(f"[RISK_FC] Error fetching withdraw timestamp: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_last_login_before_withdraw_ms(user_code, withdraw_ts_ms):
    """
    Returns the create_at (ms) of the last login BEFORE (or equal to) the withdraw timestamp.
    Uses rt.login_history.
    """
    if withdraw_ts_ms is None:
        return None

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            SELECT create_at
            FROM rt.login_history
            WHERE user_code = %s
              AND create_at <= %s
            ORDER BY create_at DESC
            LIMIT 1
        """
        cur.execute(sql, (str(user_code), int(withdraw_ts_ms)))
        row = cur.fetchone()
        if row and row[0]:
            return int(row[0])
        return None
    except Exception as e:
        print(f"[RISK_FC] Error fetching last login from rt.login_history: {e}")
        return None
    finally:
        if conn:
            conn.close()


def compute_time_since_user_login_minutes(user_code, txn_id):
    """
    Returns minutes between last login and withdraw for this txn.
    - If no withdraw or no login: returns a big safe value (999999).
    """
    try:
        withdraw_ts = get_withdraw_timestamp_ms(user_code, txn_id)
        if withdraw_ts is None:
            print(
                f"[RISK_FC] No withdraw timestamp for user={user_code}, txn={txn_id}"
            )
            return 999999

        last_login_ts = get_last_login_before_withdraw_ms(user_code, withdraw_ts)
        if last_login_ts is None:
            print(
                f"[RISK_FC] No login before withdraw for user={user_code}, txn={txn_id}"
            )
            return 999999

        diff_ms = float(withdraw_ts) - float(last_login_ts)
        if diff_ms <= 0:
            return 999999

        minutes = int(diff_ms / 60000.0)
        return minutes
    except Exception as e:
        print(f"[RISK_FC] Error computing time_since_user_login: {e}")
        return 999999


def update_time_since_user_login(user_code, txn_id, minutes):
    """
    Persist the computed time_since_user_login into rt.risk_features.
    """
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            UPDATE rt.risk_features
            SET time_since_user_login = %s
            WHERE user_code = %s AND txn_id = %s
        """
        cur.execute(sql, (int(minutes), str(user_code), str(txn_id)))
        conn.commit()
        print(
            f"[RISK_FC] Updated time_since_user_login={minutes} for user={user_code}, txn={txn_id}"
        )
    except Exception as e:
        print(f"[RISK_FC] Failed to update time_since_user_login: {e}")
    finally:
        if conn:
            conn.close()


# ------------------------------------------
# Rules cache & decision logging
# ------------------------------------------
def load_dynamic_rules():
    global _RULES_CACHE, _LAST_CACHE_TIME
    if _RULES_CACHE is not None and (time.time() - _LAST_CACHE_TIME < RULE_CACHE_TTL):
        return _RULES_CACHE
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM rt.risk_rules WHERE status = 'ACTIVE' ORDER BY priority ASC"
        )
        rows = cur.fetchall()
        rules = []
        if rows:
            for row in rows:
                rules.append(dict_factory(cur, row))
        _RULES_CACHE = rules
        _LAST_CACHE_TIME = time.time()
        return rules
    except Exception as exc:
        print(f"[RISK_FC] Error loading rules: {exc}")
        return _RULES_CACHE if _RULES_CACHE else []
    finally:
        if conn:
            conn.close()


def log_decision_to_db(user_code, txn_id, result, features, source):
    """
    Generic logger: every decision (RULE_ENGINE, SANCTIONS, AI_AGENT, etc.)
    is written into rt.risk_withdraw_decision.
    """
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            INSERT INTO rt.risk_withdraw_decision 
            (user_code, txn_id, decision, primary_threat, confidence, narrative, features_snapshot, decision_source, llm_reasoning)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        decision = result.get("decision", "HOLD")
        threat = result.get("primary_threat", "UNKNOWN")
        narrative = result.get("narrative", "")
        llm_reasoning = narrative
        score = result.get("risk_score", 0)
        confidence = float(score) / 100.0 if score >= 0 else 1.0
        features_json = json.dumps(features, default=str)

        cur.execute(
            sql,
            (
                str(user_code),
                str(txn_id),
                decision,
                threat,
                confidence,
                narrative,
                features_json,
                source,
                llm_reasoning,
            ),
        )
        conn.commit()
        print(f"[RISK_FC] Decision logged. Source: {source}, decision={decision}")
    except Exception as exc:
        print(f"[RISK_FC] Error logging decision: {exc}")
    finally:
        if conn:
            conn.close()


def update_sanction_status(user_code, txn_id, is_sanctioned):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = "UPDATE rt.risk_features SET is_sanctioned = %s WHERE user_code = %s AND txn_id = %s"
        cur.execute(sql, (is_sanctioned, str(user_code), str(txn_id)))
        conn.commit()
        print(f"[RISK_FC] Updated DB sanctions status to {is_sanctioned}")
    except Exception as e:
        print(f"[RISK_FC] Failed to update sanctions status: {e}")
    finally:
        if conn:
            conn.close()


def update_destination_age(user_code, txn_id, age_hours):
    if age_hours is None:
        return
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            UPDATE rt.risk_features
            SET destination_age_hours = %s
            WHERE user_code = %s AND txn_id = %s
        """
        cur.execute(sql, (int(age_hours), str(user_code), str(txn_id)))
        conn.commit()
        print(
            f"[RISK_FC] Updated destination_age_hours={age_hours} for user={user_code}, txn={txn_id}"
        )
    except Exception as e:
        print(f"[RISK_FC] Failed to update destination_age_hours: {e}")
    finally:
        if conn:
            conn.close()


def _make_response(status_code, data):
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(data),
        "isBase64Encoded": False,
    }


# ==========================================
# 4. EXTERNAL ENRICHMENT (BLOCKCHAIR)
# ==========================================
def _detect_blockchair_chain(address):
    if not address:
        return None
    addr = address.strip()

    if addr.startswith("0x") and len(addr) == 42:
        return "ethereum"
    if addr.startswith("1") or addr.startswith("3") or addr.startswith("bc1"):
        return "bitcoin"
    if addr.startswith("T") and 30 <= len(addr) <= 36:
        return "tron"
    return None


def fetch_destination_age_hours(address):
    if not address or not BLOCKCHAIR_API_KEY:
        return None

    now = time.time()

    if address in _DEST_AGE_CACHE:
        age_hours, ts = _DEST_AGE_CACHE[address]
        if now - ts < DEST_AGE_CACHE_TTL:
            print(
                f"[RISK_FC] Destination age cache hit for {address}: {age_hours}h"
            )
            return age_hours

    chain = _detect_blockchair_chain(address)
    if not chain:
        print(f"[RISK_FC] Could not detect chain for address: {address}")
        return None

    url = f"{BLOCKCHAIR_BASE_URL}/{chain}/dashboards/address/{address}?key={BLOCKCHAIR_API_KEY}"
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as response:
            if response.status != 200:
                print(f"[RISK_FC] Blockchair non-200: {response.status}")
                return None

            resp_json = json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"[RISK_FC] Blockchair API error: {e}")
        return None

    data = resp_json.get("data") or {}
    if not data:
        print(f"[RISK_FC] Blockchair empty data for {address}")
        return None

    addr_key = list(data.keys())[0]
    addr_info = data.get(addr_key, {})
    if "address" in addr_info and isinstance(addr_info["address"], dict):
        addr_info = addr_info["address"]

    first_seen_str = None
    for key in [
        "first_seen_receiving",
        "first_seen_spending",
        "first_seen",
        "created_at",
    ]:
        val = addr_info.get(key)
        if val:
            first_seen_str = val
            break

    if not first_seen_str:
        print(f"[RISK_FC] No first_seen timestamp for {address}")
        return None

    try:
        dt_first = datetime.strptime(first_seen_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        first_ts = dt_first.timestamp()
    except Exception as e:
        print(
            f"[RISK_FC] Failed to parse first_seen '{first_seen_str}' for {address}: {e}"
        )
        return None

    age_seconds = now - first_ts
    if age_seconds < 0:
        age_seconds = 0

    age_hours = int(age_seconds // 3600)
    _DEST_AGE_CACHE[address] = (age_hours, now)
    print(f"[RISK_FC] Destination age for {address}: {age_hours} hours")
    return age_hours


# ==========================================
# 5. LOGIC ENGINES (SANCTIONS, RULES, AI)
# ==========================================
def check_sanctions_api(address):
    if not address or not CHAINALYSIS_API_KEY:
        return False

    now = time.time()
    if address in _SANCTIONS_CACHE:
        is_bad, ts = _SANCTIONS_CACHE[address]
        if now - ts < SANCTIONS_CACHE_TTL:
            print(f"[RISK_FC] Sanctions Cache Hit for {address}: {is_bad}")
            return is_bad

    url = f"{CHAINALYSIS_URL}/{address}"
    headers = {
        "X-API-Key": CHAINALYSIS_API_KEY,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }

    is_sanctioned = False
    TEST_BAD_ADDRESS = "19D8PHBjZH29uS1uPZ4m3sVyqqfF8UFG9o"

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                if len(data.get("identifications", [])) > 0:
                    print(f"[RISK_FC] 🚨 SANCTION HIT: {address}")
                    is_sanctioned = True
    except urllib.error.HTTPError as e:
        print(f"[RISK_FC] Sanctions HTTP Error: {e.code}")
        if e.code == 403 and address == TEST_BAD_ADDRESS:
            print("[RISK_FC] Mock Bypass: Marking TEST address as Sanctioned")
            is_sanctioned = True
    except Exception as e:
        print(f"[RISK_FC] Sanctions API Error: {e}")

    _SANCTIONS_CACHE[address] = (is_sanctioned, now)
    return is_sanctioned


def evaluate_fixed_rules(features, rules):
    """
    rt.risk_rules dynamic engine.
    Returns a dict with at least:
      - triggered: bool
      - decision: 'PASS' | 'REJECT' | 'HOLD'
      - primary_threat, risk_score, narrative
    """
    safe_locals = {}
    for k, v in features.items():
        safe_locals[k] = 0 if v is None else v

    for rule in rules:
        try:
            logic = rule["logic_expression"]
            if eval(logic, {"__builtins__": None}, safe_locals):
                print(f"[RISK_FC] Rule HIT: {rule.get('rule_name')}")
                return {
                    "triggered": True,
                    "decision": rule["action"],  # e.g. PASS / HOLD / REJECT
                    "primary_threat": "RULE_HIT",
                    "risk_score": 100,
                    "narrative": f"[Rule #{rule.get('rule_id')}] {rule.get('narrative')}",
                }
        except Exception as exc:
            print(f"[RISK_FC] Error evaluating rule: {exc}")
            continue
    return {"triggered": False}


def call_gemini_reasoning_rest(features):
    """
    Phase-2 AI Agent. Only called for HOLD / grey cases.
    """
    if not GEMINI_API_KEY:
        return {
            "decision": "PASS",
            "primary_threat": "NONE",
            "narrative": "AI Config Missing",
            "risk_score": 0,
        }
    try:
        features_str = json.dumps(features, indent=2, default=str)
        full_text_prompt = (
            f"{COMPREHENSIVE_REASONING_PROMPT}\n\nUser Features:\n{features_str}"
        )

        api_url = f"https://generativelanguage.googleapis.com/v1/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        payload = {"contents": [{"parts": [{"text": full_text_prompt}]}]}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            api_url, data=data, headers={"Content-Type": "application/json"}
        )

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    if response.status == 200:
                        resp_json = json.loads(response.read().decode("utf-8"))
                        candidates = resp_json.get("candidates", [])
                        if not candidates:
                            return {
                                "decision": "PASS",
                                "primary_threat": "NONE",
                                "narrative": "AI returned no candidates",
                                "risk_score": 0,
                            }
                        raw_text = (
                            candidates[0]
                            .get("content", {})
                            .get("parts", [])[0]
                            .get("text", "")
                        )
                        clean_text = (
                            raw_text.strip()
                            .replace("```json", "")
                            .replace("```", "")
                            .strip()
                        )
                        return json.loads(clean_text)
            except urllib.error.HTTPError as e:
                print(f"[RISK_FC] HTTP Error (Gemini): {e.code}")
                time.sleep(1)
            except Exception as e:
                print(f"[RISK_FC] Gemini error attempt {attempt+1}: {e}")
                time.sleep(1)

        return {
            "decision": "HOLD",
            "primary_threat": "AI_NET_ERR",
            "narrative": "AI Unavailable",
            "risk_score": -1,
        }
    except Exception as exc:
        return {
            "decision": "HOLD",
            "primary_threat": "AI_ERR",
            "narrative": str(exc),
            "risk_score": -1,
        }


# ==========================================
# 6. NEW: DIRECT RULE ENGINE HELPERS
# ==========================================
def check_user_whitelist(user_code):
    """
    rt.risk_whitelist_user → DIRECT PASS.
    """
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = """
            SELECT 1
            FROM rt.risk_whitelist_user
            WHERE user_code = %s
              AND status = 'ACTIVE'
              AND (expires_at IS NULL OR expires_at > now())
            LIMIT 1
        """
        cur.execute(sql, (str(user_code),))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"[RISK_FC] Error checking user whitelist: {e}")
        return False
    finally:
        if conn:
            conn.close()


def check_address_whitelist(destination_address, chain=None):
    """
    rt.risk_whitelist_address → DIRECT PASS.
    """
    if not destination_address:
        return False
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        if chain:
            sql = """
                SELECT 1
                FROM rt.risk_whitelist_address
                WHERE destination_address = %s
                  AND (chain IS NULL OR chain = %s)
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """
            cur.execute(sql, (destination_address, chain))
        else:
            sql = """
                SELECT 1
                FROM rt.risk_whitelist_address
                WHERE destination_address = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """
            cur.execute(sql, (destination_address,))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"[RISK_FC] Error checking address whitelist: {e}")
        return False
    finally:
        if conn:
            conn.close()


def evaluate_low_risk_behavior(features):
    """
    Consistent & Low-Risk Behavior:
    is_new_device == False
    and is_new_ip == False
    and is_new_destination_address == False
    and account_maturity > 7
    and withdrawal_amount < 5000 (assume USD/USDT)
    """
    try:
        is_new_device = bool(features.get("is_new_device", False))
        is_new_ip = bool(features.get("is_new_ip", False))
        is_new_dest = bool(features.get("is_new_destination_address", False))

        # support account_maturity or account_maturity_days
        account_maturity = (
            features.get("account_maturity_days")
            if features.get("account_maturity_days") is not None
            else features.get("account_maturity")
        )
        if account_maturity is None:
            return None

        withdrawal_amount = (
            features.get("withdrawal_amount_usd")
            if features.get("withdrawal_amount_usd") is not None
            else features.get("withdrawal_amount")
        )
        if withdrawal_amount is None:
            return None

        if (
            not is_new_device
            and not is_new_ip
            and not is_new_dest
            and float(account_maturity) > 7
            and float(withdrawal_amount) < 5000
        ):
            return {
                "decision": "PASS",
                "primary_threat": "NONE",
                "risk_score": 0,
                "narrative": "Consistent & low-risk behavior: stable device/IP/address, mature account, small amount.",
            }
        return None
    except Exception as e:
        print(f"[RISK_FC] Error evaluating low-risk behavior: {e}")
        return None


def extract_email_domain(features):
    email = features.get("user_email") or features.get("email")
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].lower().strip()


def check_blacklists(features):
    """
    Direct REJECT rules:
    - rt.risk_blacklist_user
    - rt.risk_blacklist_address
    - rt.risk_blacklist_fingerprint
    - rt.risk_blacklist_ip
    - rt.risk_blacklist_emaildomain
    Returns a result dict or None.
    """
    user_code = features.get("user_code")
    dest_addr = features.get("destination_address")
    chain = features.get("chain")
    device_fp = features.get("device_fingerprint")
    ip_addr = features.get("ip_address") or features.get("client_ip")
    email_domain = extract_email_domain(features)

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        # 1) User blacklist
        if user_code:
            cur.execute(
                """
                SELECT reason
                FROM rt.risk_blacklist_user
                WHERE user_code = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """,
                (str(user_code),),
            )
            row = cur.fetchone()
            if row:
                reason = row[0] or "User in blacklist."
                return {
                    "decision": "REJECT",
                    "primary_threat": "BLACKLIST",
                    "risk_score": 100,
                    "narrative": f"Blacklisted user_code: {reason}",
                }

        # 2) Destination address blacklist
        if dest_addr:
            if chain:
                cur.execute(
                    """
                    SELECT reason
                    FROM rt.risk_blacklist_address
                    WHERE destination_address = %s
                      AND (chain IS NULL OR chain = %s)
                      AND status = 'ACTIVE'
                      AND (expires_at IS NULL OR expires_at > now())
                    LIMIT 1
                """,
                    (dest_addr, chain),
                )
            else:
                cur.execute(
                    """
                    SELECT reason
                    FROM rt.risk_blacklist_address
                    WHERE destination_address = %s
                      AND status = 'ACTIVE'
                      AND (expires_at IS NULL OR expires_at > now())
                    LIMIT 1
                """,
                    (dest_addr,),
                )
            row = cur.fetchone()
            if row:
                reason = row[0] or "Destination address in blacklist."
                return {
                    "decision": "REJECT",
                    "primary_threat": "BLACKLIST",
                    "risk_score": 100,
                    "narrative": f"Blacklisted destination address: {reason}",
                }

        # 3) Device fingerprint blacklist
        if device_fp:
            cur.execute(
                """
                SELECT reason
                FROM rt.risk_blacklist_fingerprint
                WHERE device_fingerprint = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """,
                (device_fp,),
            )
            row = cur.fetchone()
            if row:
                reason = row[0] or "Device fingerprint in blacklist."
                return {
                    "decision": "REJECT",
                    "primary_threat": "BLACKLIST",
                    "risk_score": 100,
                    "narrative": f"Blacklisted device fingerprint: {reason}",
                }

        # 4) IP blacklist
        if ip_addr:
            cur.execute(
                """
                SELECT reason
                FROM rt.risk_blacklist_ip
                WHERE ip_address = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """,
                (ip_addr,),
            )
            row = cur.fetchone()
            if row:
                reason = row[0] or "IP address in blacklist."
                return {
                    "decision": "REJECT",
                    "primary_threat": "BLACKLIST",
                    "risk_score": 100,
                    "narrative": f"Blacklisted IP address: {reason}",
                }

        # 5) Email domain blacklist
        if email_domain:
            cur.execute(
                """
                SELECT reason
                FROM rt.risk_blacklist_emaildomain
                WHERE email_domain = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """,
                (email_domain,),
            )
            row = cur.fetchone()
            if row:
                reason = row[0] or "Email domain in blacklist."
                return {
                    "decision": "REJECT",
                    "primary_threat": "BLACKLIST",
                    "risk_score": 100,
                    "narrative": f"Blacklisted email domain ({email_domain}): {reason}",
                }

        return None
    except Exception as e:
        print(f"[RISK_FC] Error checking blacklists: {e}")
        return None
    finally:
        if conn:
            conn.close()


def check_greylist(features):
    """
    Greylist → HOLD (Phase-1), then AI Agent (Phase-2).
    Uses rt.risk_greylist with entity_type:
      USER_CODE, IP_ADDRESS, DEVICE_FINGERPRINT, DESTINATION_ADDRESS, EMAIL_DOMAIN
    """
    user_code = features.get("user_code")
    dest_addr = features.get("destination_address")
    device_fp = features.get("device_fingerprint")
    ip_addr = features.get("ip_address") or features.get("client_ip")
    email_domain = extract_email_domain(features)

    entities = []
    if user_code:
        entities.append(("USER_CODE", str(user_code)))
    if ip_addr:
        entities.append(("IP_ADDRESS", ip_addr))
    if device_fp:
        entities.append(("DEVICE_FINGERPRINT", device_fp))
    if dest_addr:
        entities.append(("DESTINATION_ADDRESS", dest_addr))
    if email_domain:
        entities.append(("EMAIL_DOMAIN", email_domain))

    if not entities:
        return None

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        for entity_type, entity_value in entities:
            cur.execute(
                """
                SELECT reason
                FROM rt.risk_greylist
                WHERE entity_type = %s
                  AND entity_value = %s
                  AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > now())
                LIMIT 1
            """,
                (entity_type, entity_value),
            )
            row = cur.fetchone()
            if row:
                reason = row[0] or "Entity in greylist."
                return {
                    "decision": "HOLD",
                    "primary_threat": "GREYLIST",
                    "risk_score": 80,
                    "narrative": f"Greylist hit on {entity_type}: {entity_value}. Reason: {reason}",
                }

        return None
    except Exception as e:
        print(f"[RISK_FC] Error checking greylist: {e}")
        return None
    finally:
        if conn:
            conn.close()


# ==========================================
# 7. HANDLER
# ==========================================
def handler(event, context):
    print("[RISK_FC] Handler invoked")
    payload = {}
    user_code = None
    txn_id_input = None

    try:
        # Normalize event to string
        if isinstance(event, (bytes, bytearray)):
            event_str = event.decode("utf-8", errors="ignore")
        else:
            event_str = event if isinstance(event, str) else json.dumps(event)

        print("[RISK_FC] Raw event snippet:", event_str[:500])

        try:
            envelope = json.loads(event_str)
        except Exception as e:
            print("[RISK_FC] JSON parse failed:", e)
            envelope = None

        # 1) Kafka trigger path
        if (
            isinstance(envelope, list)
            and len(envelope) > 0
            and isinstance(envelope[0], dict)
            and "value" in envelope[0]
        ):
            print("[RISK_FC] Detected Kafka trigger event")
            rec = envelope[0]
            raw_val = rec.get("value")

            canal_obj = None
            if isinstance(raw_val, str):
                try:
                    decoded = base64.b64decode(raw_val).decode("utf-8")
                    canal_obj = json.loads(decoded)
                    print("[RISK_FC] Kafka value decoded from base64")
                except Exception:
                    try:
                        canal_obj = json.loads(raw_val)
                        print("[RISK_FC] Kafka value treated as plain JSON")
                    except Exception as e2:
                        print("[RISK_FC] Failed to parse Kafka value:", e2)
            elif isinstance(raw_val, dict):
                canal_obj = raw_val

            if not canal_obj:
                print("[RISK_FC] No valid Canal JSON in Kafka record, skipping")
                return "SKIPPED_INVALID_VALUE"

            print("[RISK_FC] Canal JSON snippet:", str(canal_obj)[:300])

            if canal_obj.get("type") and canal_obj.get("type") != "INSERT":
                print("[RISK_FC] Not an INSERT event, skipping")
                return "SKIPPED_NON_INSERT"

            data_list = canal_obj.get("data") or []
            if not data_list:
                print("[RISK_FC] Canal JSON has empty data[], skipping")
                return "SKIPPED_EMPTY_DATA"

            data_row = data_list[0]

            user_code = data_row.get("user_code") or data_row.get("userCode")
            txn_id_input = (
                data_row.get("code")
                or data_row.get("transaction_id")
                or data_row.get("id")
            )

            print(
                f"[RISK_FC] Kafka payload extracted: user_code={user_code}, txn_id={txn_id_input}"
            )

            if not user_code:
                print("[RISK_FC] No user_code in Kafka data row, skipping")
                return "SKIPPED_NO_USER_CODE"

            payload = {"user_code": user_code, "txn_id": txn_id_input}

        # 2) HTTP / API Gateway path
        else:
            print("[RISK_FC] Non-Kafka event, using HTTP-style parsing")
            if isinstance(envelope, dict) and "body" in envelope:
                body_str = envelope.get("body") or ""
                if envelope.get("isBase64Encoded", False) and body_str:
                    body_str = base64.b64decode(body_str).decode(
                        "utf-8", errors="ignore"
                    )
                try:
                    payload = json.loads(body_str)
                except Exception:
                    form_data = parse_qs(body_str)
                    for k, v in form_data.items():
                        payload[k] = v[0]
            elif isinstance(envelope, dict):
                payload = envelope
            else:
                try:
                    payload = json.loads(event_str)
                except Exception:
                    payload = {}

            user_code = payload.get("user_code")
            txn_id_input = (
                payload.get("txn_id")
                or payload.get("txnId")
                or payload.get("code")
                or payload.get("id")
            )

        if not user_code:
            return _make_response(400, {"error": "Missing user_code"})

    except Exception as exc:
        print("[RISK_FC] Request parsing failed:", exc)
        return _make_response(
            400, {"error": f"Request Parsing Failed: {str(exc)}"}
        )

    # ==========================================
    # STEP 1: Fetch risk_features
    # ==========================================
    features = None
    if txn_id_input:
        features = wait_for_risk_features(
            user_code, txn_id_input, max_retries=5, delay=1.0
        )

    # fallback: latest txn for this user
    if not features:
        conn = None
        try:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM rt.risk_features WHERE user_code = %s ORDER BY update_time DESC LIMIT 1",
                (str(user_code),),
            )
            row = cur.fetchone()
            if row:
                features = dict_factory(cur, row)
                print(
                    "[RISK_FC] Fallback to latest risk_features for user_code",
                    user_code,
                )
        except Exception as e:
            print("[RISK_FC] Error in fallback feature fetch:", e)
        finally:
            if conn:
                conn.close()

    if not features:
        print(
            f"[RISK_FC] No risk_features found for user_code={user_code}, txn_id={txn_id_input}"
        )
        result_payload = {
            "user_code": user_code,
            "txn_id": txn_id_input,
            "decision": "HOLD",
            "reasons": ["Risk Data Not Found"],
            "primary_threat": "UNKNOWN",
            "risk_score": 0,
            "source": "NO_DATA",
        }
        # Log as a HOLD (safety)
        log_decision_to_db(
            user_code,
            txn_id_input,
            {
                "decision": "HOLD",
                "primary_threat": "UNKNOWN",
                "risk_score": 0,
                "narrative": "Risk data not found in rt.risk_features.",
            },
            {},
            "NO_DATA",
        )
        send_lark_notification(result_payload)
        return _make_response(200, result_payload)

    final_txn_id = features.get(
        "txn_id", str(txn_id_input) if txn_id_input else "unknown"
    )

    # ensure user_code & txn_id present in features snapshot
    features["user_code"] = user_code
    features["txn_id"] = final_txn_id

    # ==========================================
    # STEP 2: DIRECT PASS (whitelist + low-risk behavior)
    # ==========================================
    dest_address = features.get("destination_address")
    chain = features.get("chain")

    # 2.1 User whitelist
    try:
        if check_user_whitelist(user_code):
            final_result = {
                "decision": "PASS",
                "primary_threat": "NONE",
                "risk_score": 0,
                "narrative": "Direct PASS: user_code is in rt.risk_whitelist_user.",
            }
            source = "RULE_ENGINE_WHITELIST_USER"
            log_decision_to_db(user_code, final_txn_id, final_result, features, source)
            result_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": "PASS",
                "reasons": [final_result["narrative"]],
                "risk_score": 0,
                "primary_threat": "NONE",
                "source": source,
            }
            # No Lark for PASS
            return _make_response(200, result_payload)
    except Exception as e:
        print(f"[RISK_FC] Error in user whitelist check: {e}")

    # 2.2 Destination address whitelist
    try:
        if dest_address and check_address_whitelist(dest_address, chain):
            final_result = {
                "decision": "PASS",
                "primary_threat": "NONE",
                "risk_score": 0,
                "narrative": "Direct PASS: destination address is in rt.risk_whitelist_address.",
            }
            source = "RULE_ENGINE_WHITELIST_ADDRESS"
            log_decision_to_db(user_code, final_txn_id, final_result, features, source)
            result_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": "PASS",
                "reasons": [final_result["narrative"]],
                "risk_score": 0,
                "primary_threat": "NONE",
                "source": source,
            }
            return _make_response(200, result_payload)
    except Exception as e:
        print(f"[RISK_FC] Error in address whitelist check: {e}")

    # 2.3 Consistent & Low-Risk Behavior
    low_risk_result = evaluate_low_risk_behavior(features)
    if low_risk_result:
        source = "RULE_ENGINE_LOW_RISK"
        log_decision_to_db(user_code, final_txn_id, low_risk_result, features, source)
        result_payload = {
            "user_code": user_code,
            "txn_id": final_txn_id,
            "decision": "PASS",
            "reasons": [low_risk_result["narrative"]],
            "risk_score": low_risk_result.get("risk_score", 0),
            "primary_threat": low_risk_result.get("primary_threat", "NONE"),
            "source": source,
        }
        return _make_response(200, result_payload)

    # ==========================================
    # STEP 3: Enrich features (impossible travel, login timing)
    # ==========================================
    try:
        is_it = compute_impossible_travel(user_code)
        features["is_impossible_travel"] = is_it
        update_impossible_travel_flag(user_code, final_txn_id, is_it)
    except Exception as e:
        print(f"[RISK_FC] Error during impossible travel pipeline: {e}")

    try:
        tsul = compute_time_since_user_login_minutes(user_code, final_txn_id)
        features["time_since_user_login"] = tsul
        update_time_since_user_login(user_code, final_txn_id, tsul)
    except Exception as e:
        print(f"[RISK_FC] Error computing time_since_user_login: {e}")

    # ==========================================
    # STEP 4: Destination age + Sanctions (Direct REJECT)
    # ==========================================
    if dest_address:
        try:
            existing_age = features.get("destination_age_hours")
            if existing_age in (None, 0):
                age_hours = fetch_destination_age_hours(dest_address)
                if age_hours is not None:
                    features["destination_age_hours"] = age_hours
                    update_destination_age(user_code, final_txn_id, age_hours)
        except Exception as e:
            print(f"[RISK_FC] Error computing destination age: {e}")

        is_bad = check_sanctions_api(dest_address)
        features["is_sanctioned"] = is_bad

        if is_bad:
            print(f"[RISK_FC] SANCTION HIT DETECTED for {final_txn_id}")
            update_sanction_status(user_code, final_txn_id, True)
            final_result = {
                "decision": "REJECT",
                "primary_threat": "SANCTIONS",
                "risk_score": 100,
                "narrative": f"CRITICAL: Destination address {dest_address} is SANCTIONED.",
            }
            source = "SANCTIONS_ENGINE"
            log_decision_to_db(user_code, final_txn_id, final_result, features, source)
            result_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": "REJECT",
                "reasons": [final_result["narrative"]],
                "risk_score": 100,
                "primary_threat": "SANCTIONS",
                "source": source,
            }
            send_lark_notification(result_payload)
            return _make_response(200, result_payload)

    # ==========================================
    # STEP 5: Direct REJECT via Blacklists
    # ==========================================
    blacklist_result = check_blacklists(features)
    if blacklist_result:
        source = "RULE_ENGINE_BLACKLIST"
        log_decision_to_db(user_code, final_txn_id, blacklist_result, features, source)
        result_payload = {
            "user_code": user_code,
            "txn_id": final_txn_id,
            "decision": blacklist_result["decision"],
            "reasons": [blacklist_result["narrative"]],
            "risk_score": blacklist_result.get("risk_score", 100),
            "primary_threat": blacklist_result.get("primary_threat", "BLACKLIST"),
            "source": source,
        }
        send_lark_notification(result_payload)
        return _make_response(200, result_payload)

    # ==========================================
    # STEP 6: Greylist → HOLD + AI (Phase-2)
    # ==========================================
    grey_result = check_greylist(features)
    if grey_result:
        # Phase 1: HOLD by rule engine
        rule_source = "RULE_ENGINE_GREYLIST"
        log_decision_to_db(user_code, final_txn_id, grey_result, features, rule_source)
        rule_payload = {
            "user_code": user_code,
            "txn_id": final_txn_id,
            "decision": "HOLD",
            "reasons": [grey_result["narrative"]],
            "risk_score": grey_result.get("risk_score", 80),
            "primary_threat": grey_result.get("primary_threat", "GREYLIST"),
            "source": rule_source,
        }
        # Notify HOLD
        send_lark_notification(rule_payload)

        # Phase 2: AI Agent to confirm/downgrade HOLD
        ai_result = call_gemini_reasoning_rest(features)
        ai_source = "AI_AGENT_GREYLIST"
        log_decision_to_db(user_code, final_txn_id, ai_result, features, ai_source)

        final_decision = ai_result.get("decision", "HOLD")
        final_payload = {
            "user_code": user_code,
            "txn_id": final_txn_id,
            "decision": final_decision,
            "reasons": [ai_result.get("narrative", "AI evaluation")],
            "risk_score": ai_result.get("risk_score", 0),
            "primary_threat": ai_result.get("primary_threat", "NONE"),
            "source": ai_source,
        }
        # Optional: if AI still says HOLD, we already notified above; skip extra Lark.
        # If in future AI can say REJECT, we can add another Lark here.
        return _make_response(200, final_payload)

    # ==========================================
    # STEP 7: Dynamic rt.risk_rules (rapid_cycling, etc.)
    # ==========================================
    rules = load_dynamic_rules()
    rule_result = evaluate_fixed_rules(features, rules)

    if rule_result.get("triggered"):
        decision = rule_result.get("decision", "HOLD")
        source = "RULE_ENGINE_RULES"

        # Phase 1 log
        log_decision_to_db(user_code, final_txn_id, rule_result, features, source)

        # If rule says PASS/REJECT → final, no AI.
        if decision in ("PASS", "REJECT"):
            result_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": decision,
                "reasons": [rule_result.get("narrative", "Rule decision")],
                "risk_score": rule_result.get("risk_score", 100),
                "primary_threat": rule_result.get("primary_threat", "RULE_HIT"),
                "source": source,
            }
            if decision in ("REJECT", "HOLD"):
                send_lark_notification(result_payload)
            return _make_response(200, result_payload)

        # If rule says HOLD → send Lark + call AI
        if decision == "HOLD":
            hold_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": "HOLD",
                "reasons": [rule_result.get("narrative", "Rule HOLD")],
                "risk_score": rule_result.get("risk_score", 100),
                "primary_threat": rule_result.get("primary_threat", "RULE_HIT"),
                "source": source,
            }
            send_lark_notification(hold_payload)

            ai_result = call_gemini_reasoning_rest(features)
            ai_source = "AI_AGENT_RULE_HOLD"
            log_decision_to_db(
                user_code, final_txn_id, ai_result, features, ai_source
            )
            final_decision = ai_result.get("decision", "HOLD")
            final_payload = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": final_decision,
                "reasons": [ai_result.get("narrative", "AI evaluation")],
                "risk_score": ai_result.get("risk_score", 0),
                "primary_threat": ai_result.get("primary_threat", "NONE"),
                "source": ai_source,
            }
            # Again, for now we skip extra Lark; we already alerted on HOLD.
            return _make_response(200, final_payload)

    # ==========================================
    # STEP 8: Default PASS (no rules / lists / sanctions hit)
    # ==========================================
    default_result = {
        "decision": "PASS",
        "primary_threat": "NONE",
        "risk_score": 0,
        "narrative": "No whitelist/blacklist/greylist or dynamic rule triggered. Default PASS.",
    }
    source = "RULE_ENGINE_DEFAULT_PASS"
    log_decision_to_db(user_code, final_txn_id, default_result, features, source)

    result_payload = {
        "user_code": user_code,
        "txn_id": final_txn_id,
        "decision": "PASS",
        "reasons": [default_result["narrative"]],
        "risk_score": 0,
        "primary_threat": "NONE",
        "source": source,
    }
    # No Lark for PASS
    return _make_response(200, result_payload)
