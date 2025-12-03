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
# Blockchair Config
# -----------------------------
BLOCKCHAIR_API_KEY   = os.environ.get("BLOCKCHAIR_API_KEY", "")
BLOCKCHAIR_BASE_URL  = "https://api.blockchair.com"
DEST_AGE_CACHE_TTL   = 3600 * 6   # 6 hours

# -----------------------------
# Caching TTLs
# -----------------------------
RULE_CACHE_TTL      = 300         # 5 minutes
SANCTIONS_CACHE_TTL = 3600        # 1 hour

# -----------------------------
# Comprehensive Reasoning Prompt
# -----------------------------
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
