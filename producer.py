import time
import json
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'crypto_stream'
API_URL = "https://api.coingecko.com/api/v3/simple/price"
HEADERS = {"accept": "application/json"}
PARAMS = {
    "ids": "uniswap,toncoin,polkadot,near,render-token,cosmos,filecoin,sui,optimism,lido-dao",
    "vs_currencies": "usd",
    "include_24hr_vol": "true"
}

print("Starting producer... Polling CoinGecko every 15 seconds.")

try:
    while True:
        try:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            current_time = datetime.now(timezone.utc).isoformat()
            
            for coin_id, metrics in data.items():
                payload = {
                    "timestamp": current_time,
                    "cryptocurrency": coin_id,
                    "price_usd": float(metrics["usd"]),
                    "volume_24h_usd": float(metrics["usd_24h_vol"])
                }
                producer.send(KAFKA_TOPIC, payload)
                
            producer.flush()
            print(f"Sent 5 crypto records at {current_time}")
        except requests.exceptions.RequestException as req_err:
            print(f"API rate limit or network hiccup: {req_err}. Retrying next cycle...")
            
        time.sleep(15)

except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()
