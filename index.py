# index.py
import json
import base64
from urllib.parse import parse_qs

import core

print("[RISK_FC] System initializing - Final Production with Rule Engine v2.1 (no feature logic)")

def _make_response(status_code, data):
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(data),
        "isBase64Encoded": False,
    }


def handler(event, context):
    print("[RISK_FC] Handler invoked")
    payload      = {}
    user_code    = None
    txn_id_input = None

    # ==========================
    # 1. Parse event (Kafka / HTTP)
    # ==========================
    try:
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

        # Kafka trigger
        if (
            isinstance(envelope, list)
            and len(envelope) > 0
            and isinstance(envelope[0], dict)
            and "value" in envelope[0]
        ):
            print("[RISK_FC] Detected Kafka trigger event")
            rec     = envelope[0]
            raw_val = rec.get("value")

            canal_obj = None
            if isinstance(raw_val, str):
                try:
                    decoded   = base64.b64decode(raw_val).decode("utf-8")
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

            user_code    = data_row.get("user_code") or data_row.get("userCode")
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

        # HTTP / API style
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

            user_code    = payload.get("user_code")
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

    # ==========================
    # 2. Fetch risk_features
    # ==========================
    features = None
    if txn_id_input:
        features = core.wait_for_risk_features(
            user_code, txn_id_input, max_retries=5, delay=1.0
        )

    # Fallback: latest txn for this user
    if not features:
        conn = None
        try:
            conn = core.get_db_conn()
            cur  = conn.cursor()
            cur.execute(
                "SELECT * FROM rt.risk_features WHERE user_code = %s ORDER BY update_time DESC LIMIT 1",
                (str(user_code),),
            )
            row = cur.fetchone()
            if row:
                features = core.dict_factory(cur, row)
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

        core.log_decision_to_db(
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
        core.send_lark_notification(result_payload)
        return _make_response(200, result_payload)

    final_txn_id = features.get(
        "txn_id", str(txn_id_input) if txn_id_input else "unknown"
    )

    # Ensure user_code & txn_id in snapshot
    features["user_code"] = user_code
    features["txn_id"]    = final_txn_id

    dest_address = features.get("destination_address")
    chain        = features.get("chain")  # optional, if you store it

    

    # ==========================
    # 6. Dynamic rt.risk_rules
    # ==========================
    rules       = core.load_dynamic_rules()
    rule_result = core.evaluate_fixed_rules(features, rules)

    if rule_result.get("triggered"):
        decision = rule_result.get("decision", "HOLD")
        source   = "RULE_ENGINE_RULES"

        core.log_decision_to_db(user_code, final_txn_id, rule_result, features, source)

        # PASS / REJECT → final
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
                core.send_lark_notification(result_payload)
            return _make_response(200, result_payload)

        # HOLD → alert + AI refinement
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
            core.send_lark_notification(hold_payload)

            ai_result = core.call_gemini_reasoning_rest(features)
            ai_source = "AI_AGENT_RULE_HOLD"
            core.log_decision_to_db(
                user_code, final_txn_id, ai_result, features, ai_source
            )
            final_decision = ai_result.get("decision", "HOLD")
            final_payload  = {
                "user_code": user_code,
                "txn_id": final_txn_id,
                "decision": final_decision,
                "reasons": [ai_result.get("narrative", "AI evaluation")],
                "risk_score": ai_result.get("risk_score", 0),
                "primary_threat": ai_result.get("primary_threat", "NONE"),
                "source": ai_source,
            }
            return _make_response(200, final_payload)

    # ==========================
    # 7. Default PASS
    # ==========================
    default_result = {
        "decision": "PASS",
        "primary_threat": "NONE",
        "risk_score": 0,
        "narrative": "No whitelist/blacklist/greylist or dynamic rule triggered. Default PASS.",
    }
    source = "RULE_ENGINE_DEFAULT_PASS"
    core.log_decision_to_db(user_code, final_txn_id, default_result, features, source)

    result_payload = {
        "user_code": user_code,
        "txn_id": final_txn_id,
        "decision": "PASS",
        "reasons": [default_result["narrative"]],
        "risk_score": 0,
        "primary_threat": "NONE",
        "source": source,
    }
    return _make_response(200, result_payload)
