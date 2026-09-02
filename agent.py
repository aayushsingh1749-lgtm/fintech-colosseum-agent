import requests
import time

TRANSACTION_FEED_URL = "https://fake.jsonmockapi.com/transactions" 
EXCHANGE_RATE_API_URL = "https://api.exchangerate-api.com/v4/latest/INR"

def fetch_data_safely(url):
    """Mimics Retry Wrapper. Survives rate source slowdowns."""
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception:
            time.sleep(1)
    return {}

def run_agent():
    print("1. Authenticating and pulling data...")
    # Fetch transactions and exchange rates
    raw_transactions = fetch_data_safely(TRANSACTION_FEED_URL)
    rates_data = fetch_data_safely(EXCHANGE_RATE_API_URL)
    
    # Safely extract the list of transactions (adjust 'data' if the API uses a different key)
    transactions_list = raw_transactions.get("data", raw_transactions) if isinstance(raw_transactions, dict) else raw_transactions
    if not isinstance(transactions_list, list):
        transactions_list = []

    normalized_records = []
    
    print("2. Normalizing, Converting, and Enriching...")
    for tx in transactions_list:
        if not isinstance(tx, dict):
            continue
            
        # Degradation Defense: Handle renamed fields safely
        tx_id = tx.get("id", tx.get("tx_id", "UNKNOWN"))
        amount = float(tx.get("amount", tx.get("value", 0.0)))
        currency = str(tx.get("currency", "INR")).upper()
        merchant = tx.get("merchant") # Degradation: May be null
        timestamp = tx.get("timestamp") # Degradation: May be malformed
        
        # Requirement: Convert all non-INR amounts to a single currency (INR)
        if currency != "INR":
            # Degradation Defense: Navigate unfamiliar fields in stale rate source
            rates_dict = rates_data.get("rates", rates_data)
            conversion_rate = float(rates_dict.get(currency, 1.0))
            if conversion_rate > 0:
                amount = amount / conversion_rate # Adjust math if rates are inverted
            currency = "INR"
            
        # Requirement: Enrich with merchant category
        # (Assuming you have a way to map this, setting to UNKNOWN if null)
        merchant_category = "UNKNOWN"
        if merchant:
            merchant_category = "MAPPED_CATEGORY" # Replace with actual lookup logic if static file is provided
            
        # Requirement: Emit a normalized record with fixed schema
        normalized_records.append({
            "id": tx_id,
            "amount_inr": round(amount, 2),
            "currency": currency,
            "merchant_category": merchant_category,
            "timestamp": timestamp
        })

    print(f"Task 1 Complete: Emitted {len(normalized_records)} normalized records.")
    
    # Task 2 Problem Statement: Aggregate and detect unusual spending patterns
    suspicious_total = 0
    for record in normalized_records:
        if record["amount_inr"] > 50000: # Threshold for unusual pattern
            suspicious_total += 1
            print(f"ALERT: Unusual spending detected on ID {record['id']}")

if __name__ == "__main__":
    run_agent()
