# index.py  (risk_enrichment_worker)
# Async enrichment microservice:
# - Trigger: Kafka onebullex.cdc.withdraw_record
# - Job: Enrich (chain, address) with sanctions + age
# - Writes: rt.dim_sanctions_address, rt.dim_destination_age

import os
import json
import base64
import time
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error


print("[ENRICH_WORKER] Initializing enrichment worker...")

# ==========================
# CONFIG
# ==========================
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = int(os.environ.get("DB_PORT", "80"))
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

CHAINALYSIS_API_KEY = os.environ.get("CHAINALYSIS_API_KEY")
CHAINALYSIS_URL     = "https://public.chainalysis.com/api/v1/address"

BLOCKCHAIR_API_KEY  = os.environ.get("BLOCKCHAIR_API_KEY")
BLOCKCHAIR_BASE_URL = "https://api.blockchair.com"

# Re-enrichment threshold (how often we re-check an address)
RECHECK_INTERVAL_HOURS = 24


# ==========================
# DB HELPERS
# ==========================
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def should_refresh_sanctions(chain, address):
    """
    Returns True if we should call sanctions API for this (chain, address).
    Logic:
      - If no row => True
      - If status in ('ERROR', 'PENDING') => True
      - If last_checked_at older than RECHECK_INTERVAL_HOURS => True
      - Else => False (already fresh)
    """
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            """
            SELECT sanctions_status, last_checked_at
            FROM rt.dim_sanctions_address
            WHERE chain = %s AND destination_address = %s
            """,
            (chain, address),
        )
        row = cur.fetchone()
        if not row:
            return True

        status = row["sanctions_status"] or "PENDING"
        last_checked_at = row["last_checked_at"]

        if status in ("ERROR", "PENDING"):
            return True

        if last_checked_at is None:
            return True

        # Check recency
        now_utc = datetime.now(timezone.utc)
        if now_utc - last_checked_at > timedelta(hours=RECHECK_INTERVAL_HOURS):
            return True

        return False
    except Exception as e:
        print(f"[ENRICH_WORKER] should_refresh_sanctions error: {e}")
        # If we can't decide, better to re-check
        return True
    finally:
        if conn:
            conn.close()


def upsert_sanctions(chain, address, is_sanctioned, status, error_msg=None):
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql = """
            INSERT INTO rt.dim_sanctions_address
                (chain, destination_address, is_sanctioned, sanctions_status, last_checked_at, last_error)
            VALUES (%s, %s, %s, %s, now(), %s)
            ON CONFLICT (chain, destination_address)
            DO UPDATE SET
                is_sanctioned    = EXCLUDED.is_sanctioned,
                sanctions_status = EXCLUDED.sanctions_status,
                last_checked_at  = EXCLUDED.last_checked_at,
                last_error       = EXCLUDED.last_error
        """
        cur.execute(sql, (chain, address, is_sanctioned, status, error_msg))
        conn.commit()
        print(
            f"[ENRICH_WORKER] Upsert sanctions ({chain}, {address}) "
            f"is_sanctioned={is_sanctioned}, status={status}"
        )
    except Exception as e:
        print(f"[ENRICH_WORKER] upsert_sanctions error: {e}")
    finally:
        if conn:
            conn.close()


def should_refresh_age(chain, address):
    """
    Returns True if we should call Blockchair for this (chain, address).
    """
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            """
            SELECT age_status, last_checked_at
            FROM rt.dim_destination_age
            WHERE chain = %s AND destination_address = %s
            """,
            (chain, address),
        )
        row = cur.fetchone()
        if not row:
            return True

        status = row["age_status"] or "PENDING"
        last_checked_at = row["last_checked_at"]

        if status in ("ERROR", "PENDING"):
            return True

        if last_checked_at is None:
            return True

        now_utc = datetime.now(timezone.utc)
        if now_utc - last_checked_at > timedelta(hours=RECHECK_INTERVAL_HOURS):
            return True

        return False
    except Exception as e:
        print(f"[ENRICH_WORKER] should_refresh_age error: {e}")
        return True
    finally:
        if conn:
            conn.close()


def upsert_age(chain, address, age_hours, status, first_seen_at=None, error_msg=None):
    conn = None
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        sql = """
            INSERT INTO rt.dim_destination_age
                (chain, destination_address, destination_age_hours, age_status, first_seen_at, last_checked_at, last_error)
            VALUES (%s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (chain, destination_address)
            DO UPDATE SET
                destination_age_hours = EXCLUDED.destination_age_hours,
                age_status            = EXCLUDED.age_status,
                first_seen_at         = COALESCE(rt.dim_destination_age.first_seen_at, EXCLUDED.first_seen_at),
                last_checked_at       = EXCLUDED.last_checked_at,
                last_error            = EXCLUDED.last_error
        """
        cur.execute(sql, (chain, address, age_hours, status, first_seen_at, error_msg))
        conn.commit()
        print(
            f"[ENRICH_WORKER] Upsert age ({chain}, {address}) "
            f"age_hours={age_hours}, status={status}"
        )
    except Exception as e:
        print(f"[ENRICH_WORKER] upsert_age error: {e}")
    finally:
        if conn:
            conn.close()


# ==========================
# EXTERNAL API HELPERS
# ==========================
def call_chainalysis(address):
    """
    Call Chainalysis public API.
    Returns (is_sanctioned, error_msg or None)
    """
    if not CHAINALYSIS_API_KEY:
        print("[ENRICH_WORKER] CHAINALYSIS_API_KEY not set, skipping")
        return False, "API_KEY_MISSING"

    if not address:
        return False, "NO_ADDRESS"

    url = f"{CHAINALYSIS_URL}/{address}"
    headers = {
        "X-API-Key": CHAINALYSIS_API_KEY,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        #req = urllib.request.Request(url, headers=headers, method="GET")
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status != 200:
                return False, f"HTTP_{resp.status}"
            data = json.loads(resp.read().decode("utf-8"))
            # According to Chainalysis docs, identifications array indicates hits
            identifications = data.get("identifications", [])
            is_sanctioned = len(identifications) > 0
            return is_sanctioned, None
    except urllib.error.HTTPError as e:
        return False, f"HTTPError_{e.code}"
    except Exception as e:
        return False, f"EXC_{e}"


def map_chain_to_blockchair_chain(chain_code):
    """
    Map your withdraw_currency/chain to Blockchair chain name.
    """
    if not chain_code:
        return None

    c = chain_code.upper()
    mapping = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "TRX": "tron",      # <--- ADD THIS LINE
        "LTC": "litecoin",  # Recommended: Add Litecoin if you support it
        "BCH": "bitcoin-cash" # Recommended: Add BCH if you support it
    }
    return mapping.get(c)


def call_blockchair_for_age(chain_code, address):
    """
    Calls Blockchair to approximate destination_age_hours.
    Returns (age_hours, first_seen_at, error_msg or None)
    """
    if not BLOCKCHAIR_API_KEY:
        print("[ENRICH_WORKER] BLOCKCHAIR_API_KEY not set, skipping")
        return None, None, "API_KEY_MISSING"

    if not address:
        return None, None, "NO_ADDRESS"

    chain_name = map_chain_to_blockchair_chain(chain_code)
    if not chain_name:
        return None, None, f"UNMAPPED_CHAIN_{chain_code}"

    # Example endpoint for address dashboard
    url = f"{BLOCKCHAIR_BASE_URL}/{chain_name}/dashboards/address/{address}?key={BLOCKCHAIR_API_KEY}"
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    try:
        #req = urllib.request.Request(url, method="GET")
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status != 200:
                return None, None, f"HTTP_{resp.status}"

            data = json.loads(resp.read().decode("utf-8"))
            # Response structure depends on chain; this is a generic example:
            addr_data = data.get("data", {}).get(address)
            if not addr_data:
                # Address might be new or unknown in chain index
                return 0.0, None, None

            # Try to read a "first seen" timestamp if present
            # This is pseudo-parsing; adjust once you inspect actual response
            meta = addr_data.get("address") or addr_data.get("address_data") or {}
            first_seen_str = meta.get("first_seen_receiving") or meta.get("first_seen")
            if not first_seen_str:
                # If unknown, treat as very new (0 hours)
                return 0.0, None, None

            try:
                # Often returns ISO format; adjust format if needed
                first_seen_dt = datetime.fromisoformat(first_seen_str.replace("Z", "+00:00"))
            except Exception:
                first_seen_dt = None

            if first_seen_dt:
                now_utc = datetime.now(timezone.utc)
                delta   = now_utc - first_seen_dt
                hours   = delta.total_seconds() / 3600.0
            else:
                hours = None

            return hours, first_seen_dt, None

    except urllib.error.HTTPError as e:
        return None, None, f"HTTPError_{e.code}"
    except Exception as e:
        return None, None, f"EXC_{e}"


# ==========================
# CORE ENRICHMENT LOGIC
# ==========================
def enrich_one_withdraw_row(row):
    """
    row = single Canal JSON data row from withdraw_record
    We only care about (chain, address).
    """
    # You can refine this mapping based on your actual columns in withdraw_record
    address = row.get("address") or row.get("withdraw_address")
    if not address:
        print("[ENRICH_WORKER] No address in row, skipping")
        return "SKIP_NO_ADDRESS"

    raw_chain = (
        row.get("chain")
        or row.get("network")
        or row.get("withdraw_chain")
        or row.get("withdraw_currency")
        or "UNKNOWN"
    )
    chain = str(raw_chain).upper()

    print(f"[ENRICH_WORKER] Processing ({chain}, {address})")

    # 1) Sanctions enrichment
    if should_refresh_sanctions(chain, address):
        is_sanctioned, err = call_chainalysis(address)
        status = "CHECKED" if err is None else "ERROR"
        upsert_sanctions(chain, address, is_sanctioned, status, err)
    else:
        print("[ENRICH_WORKER] Sanctions info fresh, skipping API")

    # 2) Destination age enrichment
    if should_refresh_age(chain, address):
        age_hours, first_seen_at, err = call_blockchair_for_age(chain, address)
        status = "CHECKED" if err is None else "ERROR"

        # If we get no age info but no hard error, treat as 0 hours (very new)
        if age_hours is None and err is None:
            age_hours = 0.0

        upsert_age(chain, address, age_hours, status, first_seen_at, err)
    else:
        print("[ENRICH_WORKER] Age info fresh, skipping API")

    return "OK"


# ==========================
# FC HANDLER (Kafka Trigger)
# ==========================
def handler(event, context):
    """
    Kafka trigger passes a batch of records (list with 'value' field).
    We parse each, decode Canal JSON, and enrich.
    """
    try:
        # Convert event to string
        if isinstance(event, (bytes, bytearray)):
            event_str = event.decode("utf-8", errors="ignore")
        else:
            event_str = event if isinstance(event, str) else json.dumps(event)

        print("[ENRICH_WORKER] Raw event snippet:", event_str[:500])

        try:
            envelope = json.loads(event_str)
        except Exception as e:
            print("[ENRICH_WORKER] JSON parse failed:", e)
            return "INVALID_EVENT"

        # FC Kafka trigger typically sends a list of records
        if not isinstance(envelope, list):
            print("[ENRICH_WORKER] Envelope is not a list, skipping")
            return "SKIP_NOT_LIST"

        results = []
        for rec in envelope:
            raw_val = rec.get("value")
            if raw_val is None:
                continue

            canal_obj = None
            if isinstance(raw_val, str):
                # Try base64 first
                try:
                    decoded = base64.b64decode(raw_val).decode("utf-8")
                    canal_obj = json.loads(decoded)
                    print("[ENRICH_WORKER] Decoded Kafka value from base64")
                except Exception:
                    try:
                        canal_obj = json.loads(raw_val)
                        print("[ENRICH_WORKER] Kafka value as plain JSON")
                    except Exception as e2:
                        print("[ENRICH_WORKER] Failed to parse Kafka value:", e2)
                        continue
            elif isinstance(raw_val, dict):
                canal_obj = raw_val

            if not canal_obj:
                print("[ENRICH_WORKER] No valid Canal JSON, skipping record")
                continue

            if canal_obj.get("type") and canal_obj.get("type") != "INSERT":
                print("[ENRICH_WORKER] Not an INSERT event, skipping")
                continue

            data_list = canal_obj.get("data") or []
            if not data_list:
                print("[ENRICH_WORKER] Canal JSON has empty data[], skipping")
                continue

            # We can process all rows in the batch (usually it's 1, but be safe)
            for row in data_list:
                try:
                    r = enrich_one_withdraw_row(row)
                    results.append(r)
                except Exception as e:
                    print(f"[ENRICH_WORKER] Error enriching row: {e}")
                    results.append("ERROR_ROW")

        # Simple aggregated result
        return f"Processed {len(results)} rows"

    except Exception as e:
        print(f"[ENRICH_WORKER] Handler error: {e}")
        return "ERROR"
