import requests
import time

# Replace this with the specific FinTech data URL or API endpoint provided in your instructions
API_URL = "https://api.example-fintech.com/transactions" 

def fetch_with_retry(url, retries=3):
    """Mimics your Retry and Backoff Wrapper to survive timeouts."""
    for i in range(retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Source error, retrying... ({i+1}/{retries})")
            time.sleep(2) 
    return []

def validate_and_inspect(raw_data):
    """Mimics your Schema Inspector & Input Validator to survive corrupted/renamed fields."""
    clean_data = []
    for item in raw_data:
        if not isinstance(item, dict):
            continue
        
        try:
            transaction = {
                "id": item.get("id", item.get("tx_id", item.get("transactionId", "UNKNOWN"))),
                "amount": float(item.get("amount", item.get("value", 0.0))),
                "currency": str(item.get("currency", "USD")).upper(),
                "memo": str(item.get("memo", item.get("description", "")))
            }
            clean_data.append(transaction)
        except ValueError:
            continue 
            
    return clean_data

def apply_guardrail(data):
    """Mimics your Air-Gap Guardrail to strip hidden sabotage instructions."""
    for item in data:
        memo = item["memo"].lower()
        if "ignore" in memo or "instruction" in memo or "system" in memo:
            item["memo"] = "[SANITIZED]"
    return data

def run_agent():
    print("Initializing FinTech Pipeline...")
    
    # Task 1: Integration (Pulling the data)
    raw_transactions = fetch_with_retry(API_URL)
    if not raw_transactions:
        print("Pipeline failed to fetch data. Check API_URL.")
        return

    # Task 2: Orchestration (Surviving the degradations)
    normalized_data = validate_and_inspect(raw_transactions)
    safe_data = apply_guardrail(normalized_data)
    
    # Final Output: The actual FinTech problem statement logic
    print(f"Successfully processed {len(safe_data)} records.")
    
    flagged_count = 0
    for tx in safe_data:
        if tx["amount"] > 5000:
            print(f"ALERT: Suspicious transaction {tx['id']} for {tx['amount']} {tx['currency']}")
            flagged_count += 1
            
    print(f"Run complete. Flagged {flagged_count} anomalies.")

if __name__ == "__main__":
    run_agent()
