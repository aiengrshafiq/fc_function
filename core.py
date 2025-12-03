# core.py
# All helper logic used by index.handler
# IMPORTANT: No feature calculations here, only reading and decisions.

import json
import time
import psycopg2
import urllib.request
import urllib.error
from datetime import datetime, timezone   # <-- ADD THIS

import config as cfg

print("[RISK_FC] Loading core.py")

_RULES_CACHE     = None
_LAST_CACHE_TIME = 0

# Sanctions & destination-age caches (lightweight in-memory)
_SANCTIONS_CACHE = {}
_DEST_AGE_CACHE  = {}


# ==========================
# DB HELPERS
# ==========================
def get_db_conn():
    return psycopg2.connect(
        host=cfg.DB_HOST,
        port=cfg.DB_PORT,
        database=cfg.DB_NAME,
        user=cfg.DB_USER,
        password=cfg.DB_PASS,
    )


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col.name] = row[idx]
    return d


def fetch_risk_features(user_code, txn_id):
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql  = "SELECT * FROM rt.risk_features WHERE user_code = %s AND txn_id = %s"
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
    No feature logic, just retrying fetch.
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


# ==========================
# LARK NOTIFICATION
# ==========================
def send_lark_notification(data):
    """
    Sends a rich card message to Lark for HOLD/REJECT.
    """
    webhook = cfg.LARK_WEBHOOK_URL
    if not webhook:
        return

    decision = data.get("decision", "HOLD")
    if decision not in ("REJECT", "HOLD"):
        return

    try:
        color       = "red"
        title_emoji = "🚨"

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
            webhook,
            data=json.dumps(card_content).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=2) as response:
            if response.status == 200:
                print("[RISK_FC] Lark notification sent.")
    except Exception as e:
        print(f"[RISK_FC] Lark Notification Error (Ignored): {e}")


# ==========================
# RULES CACHE & DECISION LOGGING
# ==========================
def load_dynamic_rules():
    global _RULES_CACHE, _LAST_CACHE_TIME
    if _RULES_CACHE is not None and (time.time() - _LAST_CACHE_TIME < cfg.RULE_CACHE_TTL):
        return _RULES_CACHE

    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM rt.risk_rules WHERE status = 'ACTIVE' ORDER BY priority ASC"
        )
        rows  = cur.fetchall()
        rules = []
        if rows:
            for row in rows:
                rules.append(dict_factory(cur, row))
        _RULES_CACHE     = rules
        _LAST_CACHE_TIME = time.time()
        return rules
    except Exception as exc:
        print(f"[RISK_FC] Error loading rules: {exc}")
        return _RULES_CACHE if _RULES_CACHE else []
    finally:
        if conn:
            conn.close()


def log_decision_to_db(user_code, txn_id, result, features, source):
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql  = """
            INSERT INTO rt.risk_withdraw_decision 
            (user_code, txn_id, decision, primary_threat, confidence, narrative, features_snapshot, decision_source, llm_reasoning)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        decision      = result.get("decision", "HOLD")
        threat        = result.get("primary_threat", "UNKNOWN")
        narrative     = result.get("narrative", "")
        llm_reasoning = narrative
        score         = result.get("risk_score", 0)
        confidence    = float(score) / 100.0 if score >= 0 else 1.0
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


# ==========================
# RULE EVALUATION (rt.risk_rules)
# ==========================
def evaluate_fixed_rules(features, rules):
    """
    Evaluate rules from rt.risk_rules based purely on features in rt.risk_features.
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
                    "decision": rule["action"],  # PASS / HOLD / REJECT
                    "primary_threat": "RULE_HIT",
                    "risk_score": 100,
                    "narrative": f"[Rule #{rule.get('rule_id')}] {rule.get('narrative')}",
                }
        except Exception as exc:
            print(f"[RISK_FC] Error evaluating rule: {exc}")
            continue
    return {"triggered": False}


# ==========================
# AI AGENT
# ==========================
def call_gemini_reasoning_rest(features):
    if not cfg.GEMINI_API_KEY:
        return {
            "decision": "PASS",
            "primary_threat": "NONE",
            "narrative": "AI Config Missing",
            "risk_score": 0,
        }

    try:
        features_str     = json.dumps(features, indent=2, default=str)
        full_text_prompt = f"{cfg.COMPREHENSIVE_REASONING_PROMPT}\n\nUser Features:\n{features_str}"

        api_url = (
            f"https://generativelanguage.googleapis.com/v1/models/"
            f"{cfg.GEMINI_MODEL}:generateContent?key={cfg.GEMINI_API_KEY}"
        )
        payload = {"contents": [{"parts": [{"text": full_text_prompt}]}]}
        data    = json.dumps(payload).encode("utf-8")
        req     = urllib.request.Request(
            api_url, data=data, headers={"Content-Type": "application/json"}
        )

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    if response.status == 200:
                        resp_json  = json.loads(response.read().decode("utf-8"))
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


# ==========================
# WHITELIST / BLACKLIST / GREYLIST
# (list lookups only – no feature calculations)
# ==========================
def check_user_whitelist(user_code):
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql  = """
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
    if not destination_address:
        return False

    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
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


def extract_email_domain(features):
    email = features.get("user_email") or features.get("email")
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].lower().strip()


def check_blacklists(features):
    """
    Direct REJECT via blacklist tables.
    """
    user_code    = features.get("user_code")
    dest_addr    = features.get("destination_address")
    device_fp    = features.get("device_fingerprint")
    ip_addr      = features.get("ip_address") or features.get("client_ip")
    email_domain = extract_email_domain(features)

    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()

        # user blacklist
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

        # dest address blacklist
        if dest_addr:
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

        # device fingerprint blacklist
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

        # IP blacklist
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

        # email domain blacklist
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
    """
    user_code    = features.get("user_code")
    dest_addr    = features.get("destination_address")
    device_fp    = features.get("device_fingerprint")
    ip_addr      = features.get("ip_address") or features.get("client_ip")
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
        cur  = conn.cursor()
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



# ==========================
# SANCTIONS & DESTINATION AGE (temporary synchronous enrichment)
# ==========================

def check_sanctions_api(address: str) -> bool:
    """
    Synchronous sanctions screening via Chainalysis.
    Returns True if address is sanctioned / flagged.
    Uses a simple in-memory cache to avoid hammering the API.
    """
    if not address or not cfg.CHAINALYSIS_API_KEY:
        # If API key is missing, we silently treat as "not sanctioned"
        # (better than breaking the FC; you'll see this in logs)
        print("[RISK_FC] Sanctions API key missing or empty; skipping sanctions check.")
        return False

    now = time.time()
    ttl = getattr(cfg, "SANCTIONS_CACHE_TTL", 3600)

    # Cache hit?
    if address in _SANCTIONS_CACHE:
        is_bad, ts = _SANCTIONS_CACHE[address]
        if now - ts < ttl:
            print(f"[RISK_FC] Sanctions Cache Hit for {address}: {is_bad}")
            return is_bad

    url = f"{cfg.CHAINALYSIS_URL}/{address}"
    headers = {
        "X-API-Key": cfg.CHAINALYSIS_API_KEY,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }

    is_sanctioned = False

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                if len(data.get("identifications", [])) > 0:
                    print(f"[RISK_FC] 🚨 SANCTION HIT: {address}")
                    is_sanctioned = True
            else:
                print(f"[RISK_FC] Sanctions non-200: {response.status}")
    except urllib.error.HTTPError as e:
        print(f"[RISK_FC] Sanctions HTTP Error: {e.code}")
    except Exception as e:
        print(f"[RISK_FC] Sanctions API Error: {e}")

    _SANCTIONS_CACHE[address] = (is_sanctioned, now)
    return is_sanctioned


def fetch_destination_age_hours(address: str):
    """
    Uses Blockchair to estimate how "old" the destination address is (in hours)
    based on first_seen timestamps. This is a temporary synchronous enrichment,
    until you move it to a dim + async job.
    """
    if not address or not cfg.BLOCKCHAIR_API_KEY:
        return None

    now   = time.time()
    ttl   = getattr(cfg, "DEST_AGE_CACHE_TTL", 21600)  # default 6h
    cache = _DEST_AGE_CACHE

    if address in cache:
        age_hours, ts = cache[address]
        if now - ts < ttl:
            print(f"[RISK_FC] Destination age cache hit for {address}: {age_hours}h")
            return age_hours

    # Very simple chain detection based on address pattern
    def _detect_blockchair_chain(addr: str):
        addr = addr.strip()
        if addr.startswith("0x") and len(addr) == 42:
            return "ethereum"
        if addr.startswith("1") or addr.startswith("3") or addr.startswith("bc1"):
            return "bitcoin"
        if addr.startswith("T") and 30 <= len(addr) <= 36:
            return "tron"
        return None

    chain = _detect_blockchair_chain(address)
    if not chain:
        print(f"[RISK_FC] Could not detect chain for address: {address}")
        return None

    url = f"{cfg.BLOCKCHAIR_BASE_URL}/{chain}/dashboards/address/{address}?key={cfg.BLOCKCHAIR_API_KEY}"
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
    for key in ["first_seen_receiving", "first_seen_spending", "first_seen", "created_at"]:
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
        print(f"[RISK_FC] Failed to parse first_seen '{first_seen_str}' for {address}: {e}")
        return None

    age_seconds = now - first_ts
    if age_seconds < 0:
        age_seconds = 0

    age_hours = int(age_seconds // 3600)
    cache[address] = (age_hours, now)
    print(f"[RISK_FC] Destination age for {address}: {age_hours} hours")
    return age_hours


def update_destination_age(user_code, txn_id, age_hours):
    """
    Persist destination_age_hours into rt.risk_features.
    This is a small DB write so that future rules / analytics can reuse it.
    """
    if age_hours is None:
        return

    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql  = """
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
