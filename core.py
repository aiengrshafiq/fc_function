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

        # NEW: read amount + token
        withdrawal_amount = data.get("withdrawal_amount")
        withdraw_token    = data.get("withdraw_currency")

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
                            # NEW: Token
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**Token:**\n{withdraw_token}",
                                },
                            },
                            # NEW: Amount
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**Amount:**\n{withdrawal_amount}",
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

        # NEW: prefer explicit confidence if provided, else derive from risk_score
        if "confidence" in result:
            try:
                confidence = float(result.get("confidence"))
            except Exception:
                confidence = 0.7
        else:
            score = result.get("risk_score", 0)
            confidence = float(score) / 100.0 if isinstance(score, (int, float)) and score >= 0 else 1.0

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
                    # NEW: pass rule metadata to AI
                    "rule_id": rule.get("rule_id"),
                    "rule_name": rule.get("rule_name"),
                }
        except Exception as exc:
            print(f"[RISK_FC] Error evaluating rule: {exc}")
            continue
    return {"triggered": False}


# ==========================
# AI AGENT
# ==========================
def call_gemini_reasoning_rest(features, rule_context=None):
    """
    Phase-2 AI Agent.

    Input:
      - features: dict from rt.risk_features
      - rule_context: dict from evaluate_fixed_rules (contains rule_id, rule_name, narrative, decision=HOLD)

    Output (dict):
      {
        "final_decision": "PASS" | "HOLD" | "REJECT",
        "primary_threat": "AML" | "SCAM" | "ATO" | "INTEGRITY" | "NONE",
        "risk_score": int 0-100,
        "confidence": float 0.0-1.0,
        "narrative": str,
        "rule_alignment": "AGREES_WITH_RULE" | "OVERRIDES_TO_PASS" | "OVERRIDES_TO_REJECT"
      }
    """
    if not cfg.GEMINI_API_KEY:
        return {
            "final_decision": "HOLD",
            "primary_threat": "NONE",
            "risk_score": 0,
            "confidence": 0.5,
            "narrative": "AI config missing. Keeping HOLD for manual review.",
            "rule_alignment": "AGREES_WITH_RULE",
        }

    try:
        case_payload = {
            "features": features,
            "rule_engine": {
                "initial_decision": (rule_context or {}).get("decision", "HOLD"),
                "rule_id": (rule_context or {}).get("rule_id"),
                "rule_name": (rule_context or {}).get("rule_name"),
                "rule_narrative": (rule_context or {}).get("narrative"),
            },
        }

        case_str          = json.dumps(case_payload, indent=2, default=str)
        full_text_prompt  = f"{cfg.COMPREHENSIVE_REASONING_PROMPT}\n\nCase JSON:\n{case_str}"

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
                            break

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
                        ai_obj = json.loads(clean_text)

                        # Normalise / validate fields & defaults
                        final_decision = ai_obj.get("final_decision", "HOLD")
                        primary_threat = ai_obj.get("primary_threat", "NONE")
                        risk_score     = int(ai_obj.get("risk_score", 0) or 0)
                        confidence     = float(ai_obj.get("confidence", 0.7) or 0.7)
                        narrative      = ai_obj.get("narrative", "AI evaluation.")
                        rule_alignment = ai_obj.get("rule_alignment", "AGREES_WITH_RULE")

                        return {
                            "final_decision": final_decision,
                            "primary_threat": primary_threat,
                            "risk_score": risk_score,
                            "confidence": confidence,
                            "narrative": narrative,
                            "rule_alignment": rule_alignment,
                        }
            except urllib.error.HTTPError as e:
                print(f"[RISK_FC] HTTP Error (Gemini): {e.code}")
                time.sleep(1)
            except Exception as e:
                print(f"[RISK_FC] Gemini error attempt {attempt+1}: {e}")
                time.sleep(1)

        # Fallback if all attempts fail or JSON is bad
        return {
            "final_decision": "HOLD",
            "primary_threat": "AI_NET_ERR",
            "risk_score": -1,
            "confidence": 0.5,
            "narrative": "AI unavailable or invalid response. Keeping HOLD for manual review.",
            "rule_alignment": "AGREES_WITH_RULE",
        }
    except Exception as exc:
        print(f"[RISK_FC] Gemini fatal error: {exc}")
        return {
            "final_decision": "HOLD",
            "primary_threat": "AI_ERR",
            "risk_score": -1,
            "confidence": 0.5,
            "narrative": f"AI exception: {str(exc)}",
            "rule_alignment": "AGREES_WITH_RULE",
        }


