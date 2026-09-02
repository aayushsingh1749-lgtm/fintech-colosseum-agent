"""
FinTech Transaction Pipeline
----------------------------
Four distinct defensive components, each independently callable/testable:

  1. Retry & Backoff Wrapper  -> fetch_with_retry()
  2. Schema Inspector         -> inspect_schema()
  3. Input Validator          -> validate_input()
  4. Air-Gap Guardrail        -> apply_guardrail()
"""

import statistics
import time

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TRANSACTION_FEED_URL = "https://fake.jsonmockapi.com/transactions"
EXCHANGE_RATE_API_URL = "https://api.exchangerate-api.com/v4/latest/INR"

BASE_CURRENCY = "INR"
ANOMALY_STD_MULTIPLIER = 3
MIN_RECORDS_FOR_STATS = 5
FALLBACK_ANOMALY_THRESHOLD = 50000

MERCHANT_CATEGORY_MAP = {
    # "amazon": "SHOPPING",
    # "uber": "TRANSPORT",
}

SUSPICIOUS_KEYWORDS = (
    "ignore", "instruction", "system", "disregard", "override",
    "prompt", "you are now", "new rule",
)


# ---------------------------------------------------------------------------
# 1. Retry & Backoff Wrapper
# ---------------------------------------------------------------------------
def fetch_with_retry(url, retries=3, base_delay=1.0, timeout=5):
    """
    Fetch JSON from `url`, retrying on failure with EXPONENTIAL backoff:
    delay doubles each attempt (base_delay, base_delay*2, base_delay*4, ...).
    Returns the parsed JSON, or None if every attempt fails.
    """
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - any source failure should retry
            last_error = exc
            delay = base_delay * (2 ** (attempt - 1))
            print(f"Source error ({url}): {exc}. Retrying in {delay:.1f}s... ({attempt}/{retries})")
            if attempt < retries:
                time.sleep(delay)
    print(f"Giving up on {url} after {retries} attempts: {last_error}")
    return None


# ---------------------------------------------------------------------------
# 2. Schema Inspector
# ---------------------------------------------------------------------------
def inspect_schema(raw_tx):
    """
    Map an incoming record's possibly-renamed fields onto a fixed set of
    canonical keys. Does NOT check value types or validity - that's the
    Input Validator's job. Returns None only if the record isn't a dict
    at all (nothing to inspect).
    """
    if not isinstance(raw_tx, dict):
        return None

    return {
        "id": raw_tx.get("id", raw_tx.get("tx_id", raw_tx.get("transactionId"))),
        "amount": raw_tx.get("amount", raw_tx.get("value")),
        "currency": raw_tx.get("currency"),
        "memo": raw_tx.get("memo", raw_tx.get("description", "")),
        "merchant": raw_tx.get("merchant"),
        "timestamp": raw_tx.get("timestamp"),
    }


# ---------------------------------------------------------------------------
# 3. Input Validator
# ---------------------------------------------------------------------------
def validate_input(mapped_tx):
    """
    Check that a schema-inspected record's values are actually usable.
    Applies defaults for missing-but-optional fields, and rejects (returns
    None) records with unusable required fields.
    """
    if mapped_tx is None:
        return None

    # id: required, must be a non-empty identifier
    tx_id = mapped_tx.get("id")
    if tx_id is None or str(tx_id).strip() == "":
        return None

    # amount: required, must parse to a finite, non-negative number
    try:
        amount = float(mapped_tx.get("amount"))
    except (TypeError, ValueError):
        return None
    if amount < 0 or amount != amount:  # NaN check
        return None

    # currency: optional, defaults to base currency, normalized to upper-case
    currency = mapped_tx.get("currency")
    currency = str(currency).upper() if currency else BASE_CURRENCY

    # memo: optional, coerced to string
    memo = str(mapped_tx.get("memo") or "")

    # merchant: optional, left as-is (None is valid)
    merchant = mapped_tx.get("merchant")

    # timestamp: optional, but if present must be a non-empty string
    timestamp = mapped_tx.get("timestamp")
    if timestamp is not None and str(timestamp).strip() == "":
        timestamp = None

    return {
        "id": tx_id,
        "amount": amount,
        "currency": currency,
        "memo": memo,
        "merchant": merchant,
        "timestamp": timestamp,
    }


# ---------------------------------------------------------------------------
# 4. Air-Gap Guardrail
# ---------------------------------------------------------------------------
def _contains_injection(text):
    lowered = str(text).lower()
    return any(keyword in lowered for keyword in SUSPICIOUS_KEYWORDS)


def apply_guardrail(validated_tx):
    """
    Strip suspected prompt-injection / instruction-hijack content from every
    free-text field an upstream source could control (memo AND merchant),
    not just memo. Sanitizes a copy; never drops the record.
    """
    tx = dict(validated_tx)
    if _contains_injection(tx["memo"]):
        tx["memo"] = "[SANITIZED]"
    if tx["merchant"] and _contains_injection(tx["merchant"]):
        tx["merchant"] = "[SANITIZED]"
    return tx


# ---------------------------------------------------------------------------
# Business logic (built on top of the four components above)
# ---------------------------------------------------------------------------
def extract_transaction_list(raw_transactions):
    if raw_transactions is None:
        return []
    if isinstance(raw_transactions, list):
        return raw_transactions
    if isinstance(raw_transactions, dict):
        for key in ("data", "transactions", "results"):
            if isinstance(raw_transactions.get(key), list):
                return raw_transactions[key]
    return []


def get_conversion_rate(rates_payload, currency):
    """exchangerate-api.com's /latest/INR gives 1 INR = rates[currency] units."""
    if currency == BASE_CURRENCY:
        return 1.0
    rates_dict = rates_payload.get("rates", {}) if isinstance(rates_payload, dict) else {}
    rate = rates_dict.get(currency)
    if not rate or rate <= 0:
        print(f"Warning: no valid rate for {currency}, leaving amount unconverted.")
        return None
    return float(rate)


def normalize_transaction(safe_tx, rates_payload):
    currency = safe_tx["currency"]
    amount = safe_tx["amount"]

    rate = get_conversion_rate(rates_payload, currency)
    amount_inr = amount if rate is None else amount / rate

    merchant = safe_tx["merchant"]
    merchant_category = (
        MERCHANT_CATEGORY_MAP.get(str(merchant).lower(), "UNKNOWN") if merchant else "UNKNOWN"
    )

    return {
        "id": safe_tx["id"],
        "amount_inr": round(amount_inr, 2),
        "original_currency": currency,
        "merchant_category": merchant_category,
        "memo": safe_tx["memo"],
        "timestamp": safe_tx["timestamp"],
    }


def detect_anomalies(records):
    amounts = [r["amount_inr"] for r in records]
    if len(amounts) >= MIN_RECORDS_FOR_STATS:
        median = statistics.median(amounts)
        mad = statistics.median([abs(a - median) for a in amounts]) or 1e-9
        # 1.4826 scales MAD to be comparable to a standard deviation for normal data
        threshold = median + ANOMALY_STD_MULTIPLIER * 1.4826 * mad
    else:
        threshold = FALLBACK_ANOMALY_THRESHOLD
    flagged = [r for r in records if r["amount_inr"] > threshold]
    return flagged, threshold


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_pipeline():
    print("1. Fetching transactions and exchange rates...")
    raw_transactions = fetch_with_retry(TRANSACTION_FEED_URL)
    rates_payload = fetch_with_retry(EXCHANGE_RATE_API_URL) or {}

    transactions_list = extract_transaction_list(raw_transactions)
    if not transactions_list:
        print("Pipeline failed: no transactions retrieved. Check TRANSACTION_FEED_URL.")
        return

    print(f"2. Inspecting schema and validating {len(transactions_list)} raw records...")
    safe_transactions = []
    dropped = 0
    for raw in transactions_list:
        mapped = inspect_schema(raw)
        validated = validate_input(mapped)
        if validated is None:
            dropped += 1
            continue
        safe_transactions.append(apply_guardrail(validated))
    print(f"   {len(safe_transactions)} passed ({dropped} dropped).")

    print("3. Normalizing currency and enriching merchant category...")
    normalized_records = [normalize_transaction(tx, rates_payload) for tx in safe_transactions]

    print("4. Detecting unusual spending patterns...")
    flagged, threshold = detect_anomalies(normalized_records)
    print(f"   Anomaly threshold: {threshold:,.2f} INR")
    for record in flagged:
        print(f"   ALERT: {record['id']} — {record['amount_inr']:,.2f} INR "
              f"(category: {record['merchant_category']})")

    print(f"\nRun complete. {len(normalized_records)} normalized, "
          f"{len(flagged)} flagged as unusual.")
    return normalized_records, flagged


if __name__ == "__main__":
    run_pipeline()
