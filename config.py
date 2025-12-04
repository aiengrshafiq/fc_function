# config.py
# Central configuration for Risk Withdraw FC

import os

print("[RISK_FC] Loading config.py")

# -----------------------------
# DB Config (use env vars)
# -----------------------------
DB_HOST = os.environ.get("DB_HOST", "YOUR_HOLOGRES_HOST")
DB_PORT = int(os.environ.get("DB_PORT", "80"))
DB_NAME = os.environ.get("DB_NAME", "onebullex_rt")
DB_USER = os.environ.get("DB_USER", "YOUR_DB_USER")
DB_PASS = os.environ.get("DB_PASS", "YOUR_DB_PASSWORD")

# -----------------------------
# AI Config (Gemini)
# -----------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# -----------------------------
# Chainalysis Config
# -----------------------------
CHAINALYSIS_API_KEY = os.environ.get("CHAINALYSIS_API_KEY", "")
CHAINALYSIS_URL     = "https://public.chainalysis.com/api/v1/address"

# -----------------------------
# Lark Config
# -----------------------------
LARK_WEBHOOK_URL = os.environ.get(
    "LARK_WEBHOOK_URL",
    "https://open.larksuite.com/open-apis/bot/v2/hook/REPLACE_ME",
)

# -----------------------------
# Blockchair & Sanctions Config
# -----------------------------



# --- Sanctions / Blockchair ---
CHAINALYSIS_API_KEY  = os.environ.get("CHAINALYSIS_API_KEY", "")
CHAINALYSIS_URL      = os.environ.get("CHAINALYSIS_URL", "https://public.chainalysis.com/api/v1/address")

BLOCKCHAIR_API_KEY   = os.environ.get("BLOCKCHAIR_API_KEY", "")
BLOCKCHAIR_BASE_URL  = os.environ.get("BLOCKCHAIR_BASE_URL", "https://api.blockchair.com")

SANCTIONS_CACHE_TTL  = int(os.environ.get("SANCTIONS_CACHE_TTL", "3600"))      # 1 hour
DEST_AGE_CACHE_TTL   = int(os.environ.get("DEST_AGE_CACHE_TTL", "21600"))     # 6 hours
RULE_CACHE_TTL      = 300         # 5 minutes

# -----------------------------
# Comprehensive Reasoning Prompt
# -----------------------------
COMPREHENSIVE_REASONING_PROMPT = """
You are the Phase 2 Risk AI Agent for OneBullEx.

The pipeline has TWO stages:

1) Phase 1: RULE ENGINE
   - It has already applied all hard checks:
     - Whitelist / blacklist
     - Sanctions hits
     - Basic anomaly rules
   - You ONLY receive transactions that the Rule Engine marked as HOLD
     (i.e. "grey area", ambiguous, complex cases).

2) Phase 2: YOU (AI Agent)
   - Your job is to re-evaluate the risk using all numeric and boolean features.
   - You must output ONE final decision:
       - "PASS"   → Safe to allow withdrawal.
       - "HOLD"   → Still ambiguous / suspicious, keep for manual review.
       - "REJECT" → High risk; block the transaction.

You will receive a JSON object with this structure:

{
  "features": { ... all risk_features columns ... },
  "rule_engine": {
    "initial_decision": "HOLD",
    "rule_id": <int or null>,
    "rule_name": "<string or null>",
    "rule_narrative": "<original rule narrative>"
  }
}

Use the features to reason about:

- AML / Money Mule / Layering
- SCAM victim behavior
- Account Takeover (ATO)
- Integrity / Exploitation patterns

IMPORTANT:
- Sanctions hits and blacklists are ALREADY handled in Phase 1 and will NOT appear here.
- Be conservative: if evidence is weak or contradictory, keep HOLD.
- If behavior is clearly benign, downgrade to PASS.
- If behavior is clearly dangerous, upgrade to REJECT.

Your output MUST be STRICT JSON with NO extra text, code fences, or commentary:

{
  "final_decision": "PASS" | "HOLD" | "REJECT",
  "primary_threat": "AML" | "SCAM" | "ATO" | "INTEGRITY" | "NONE",
  "risk_score": <integer 0-100>,
  "confidence": <float 0.0-1.0>,
  "narrative": "Short explanation in 2-4 sentences.",
  "rule_alignment": "AGREES_WITH_RULE" | "OVERRIDES_TO_PASS" | "OVERRIDES_TO_REJECT"
}

Guidance:
- If you output "REJECT", risk_score is typically >= 80.
- If you output "HOLD", risk_score is typically between 60 and 90.
- If you output "PASS", risk_score is typically < 60.

DO NOT output anything except the JSON object.
"""
