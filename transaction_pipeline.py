"""
FinTech Transaction Pipeline
----------------------------
1. Fetch raw transactions (with retry/backoff) from a source API.
2. Fetch current exchange rates (with retry/backoff).
3. Validate + normalize each transaction into a fixed schema.
4. Convert all amounts to a single base currency (INR).
5. Enrich with merchant category.
6. Detect unusual spending patterns using a statistical threshold
   (mean + N*std) rather than an arbitrary hardcoded number.
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
ANOMALY_STD_MULTIPLIER = 3  # flag amounts > mean + 3*std as unusual
MIN_RECORDS_FOR_STATS = 5   # need a reasonable sample before stats are meaningful
FALLBACK_ANOMALY_THRESHOLD = 50000  # used only if sample is too small for stats

# Static fallback mapping; replace with a real lookup (DB/file/API) when available
MERCHANT_CATEGORY_MAP = {
    # "amazon": "SHOPPING",
    # "uber": "TRANSPORT",
}

SUSPICIOUS_MEMO_KEYWORDS = ("ignore", "instruction", "system")


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
def fetch_with_retry(url, retries=3, backoff_seconds=2, timeout=5):
    """Fetch JSON from `url`, retrying on failure with linear backoff."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any source failure should retry
            last_error = exc
            print(f"Source error ({url}): {exc}. Retrying... ({attempt}/{retries})")
            time.sleep(backoff_seconds)
    print(f"Giving up on {url} after {retries} attempts: {last_error}")
    return None


# ---------------------------------------------------------------------------
# Validation / Normalization
# ---------------------------------------------------------------------------
def extract_transaction_list(raw_transactions):
    """Handle APIs that wrap the list in an envelope, e.g. {"data": [...]}."""
    if raw_transactions is None:
        return []
    if isinstance(raw_transactions, list):
        return raw_transactions
    if isinstance(raw_transactions, dict):
        for key in ("data", "transactions", "results"):
            if isinstance(raw_transactions.get(key), list):
                return raw_transactions[key]
    return []


def sanitize_memo(memo):
    """Strip anything that looks like an injected instruction in free-text fields."""
    lowered = memo.lower()
    if any(keyword in lowered for keyword in SUSPICIOUS_MEMO_KEYWORDS):
        return "[SANITIZED]"
    return memo


def validate_transaction(raw_tx):
    """
    Coerce one raw transaction dict into a clean, minimal schema.
    Returns None if the record is unusable (not a dict / bad amount).
    """
    if not isinstance(raw_tx, dict):
        return None

    try:
        amount = float(raw_tx.get("amount", raw_tx.get("value", 0.0)))
    except (TypeError, ValueError):
        return None

    tx_id = raw_tx.get("id", raw_tx.get("tx_id", raw_tx.get("transactionId", "UNKNOWN")))
    currency = str(raw_tx.get("currency", BASE_CURRENCY)).upper()
    memo = sanitize_memo(str(raw_tx.get("memo", raw_tx.get("description", ""))))
    merchant = raw_tx.get("merchant")
    timestamp = raw_tx.get("timestamp")

    return {
        "id": tx_id,
        "amount": amount,
        "currency": currency,
        "memo": memo,
        "merchant": merchant,
        "timestamp": timestamp,
    }


def get_conversion_rate(rates_payload, currency):
    """
    exchangerate-api.com's /latest/INR endpoint returns rates as:
      1 INR = rates[currency] units of `currency`.
    So converting FROM `currency` TO INR means dividing by that rate.
    """
    if currency == BASE_CURRENCY:
        return 1.0

    rates_dict = rates_payload.get("rates", {}) if isinstance(rates_payload, dict) else {}
    rate = rates_dict.get(currency)
    if not rate or rate <= 0:
        print(f"Warning: no valid rate for {currency}, leaving amount unconverted.")
        return None
    return float(rate)


def normalize_transaction(clean_tx, rates_payload):
    """Convert to INR and enrich with merchant category. Returns a fixed-schema record."""
    currency = clean_tx["currency"]
    amount = clean_tx["amount"]

    rate = get_conversion_rate(rates_payload, currency)
    if rate is None:
        amount_inr = amount  # can't convert; keep raw amount as a documented fallback
    else:
        amount_inr = amount / rate

    merchant = clean_tx["merchant"]
    merchant_category = MERCHANT_CATEGORY_MAP.get(
        str(merchant).lower(), "UNKNOWN"
    ) if merchant else "UNKNOWN"

    return {
        "id": clean_tx["id"],
        "amount_inr": round(amount_inr, 2),
        "original_currency": currency,
        "merchant_category": merchant_category,
        "memo": clean_tx["memo"],
        "timestamp": clean_tx["timestamp"],
    }


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------
def detect_anomalies(records):
    """
    Flag unusually large transactions using mean + N*std when the sample
    is big enough to make that meaningful; otherwise fall back to a fixed
    threshold so small batches still get *some* screening.
    """
    amounts = [r["amount_inr"] for r in records]

    if len(amounts) >= MIN_RECORDS_FOR_STATS:
        mean = statistics.mean(amounts)
        stdev = statistics.pstdev(amounts)
        threshold = mean + ANOMALY_STD_MULTIPLIER * stdev
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

    print(f"2. Validating {len(transactions_list)} raw records...")
    clean_transactions = [
        clean for raw in transactions_list if (clean := validate_transaction(raw)) is not None
    ]
    print(f"   {len(clean_transactions)} passed validation "
          f"({len(transactions_list) - len(clean_transactions)} dropped).")

    print("3. Normalizing currency and enriching merchant category...")
    normalized_records = [
        normalize_transaction(tx, rates_payload) for tx in clean_transactions
    ]

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
