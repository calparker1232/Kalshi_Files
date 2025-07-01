from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature
import asyncio
import websockets
import datetime
import json
import sys

sys.modules.pop("websockets", None)

print(f"Using websockets version: {websockets.__version__}")

def load_private_key_from_file(file_path):
    with open(file_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,  # If encrypted, provide the password
            backend=default_backend()
        )
    return private_key

def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    message = text.encode('utf-8')  
    try:
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    except InvalidSignature:
        raise ValueError("RSA sign PSS failed")

current_time_milliseconds = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
timestamp_str = str(current_time_milliseconds)

private_key_path = r"C:\Users\cal3p\OneDrive\Documents\kalshi.pem"
private_key = load_private_key_from_file(private_key_path)  # Load RSA key

method = "GET"
websocket_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"  # Single WebSocket endpoint

msg_string = timestamp_str + method + "/trade-api/ws/v2"
sig = sign_pss_text(private_key, msg_string)  # Sign the request

request_headers = {
    "KALSHI-ACCESS-KEY": "97970105-48d7-4f7b-a7ea-44dbace20ded",
    "KALSHI-ACCESS-SIGNATURE": sig,
    "KALSHI-ACCESS-TIMESTAMP": timestamp_str
}

async def connect_to_websocket():
    """Connects to Kalshi's WebSocket API, fetches open events, and prints NME markets."""
    try:
        async with websockets.connect(websocket_url, additional_headers=request_headers) as websocket:
            print(f"Connected to {websocket_url}")

            # Request all open events/markets (adjust 'cmd' as needed for your API)
            get_events_msg = json.dumps({
                "id": 2,
                "cmd": "get_events",  # This command may need to be changed based on actual API
                "params": {"status": "open"}
            })
            await websocket.send(get_events_msg)
            print(f"Sent message: {get_events_msg}")

            while True:
                response = await websocket.recv()
                print("Received:", response)
                try:
                    data = json.loads(response)
                    # Look for the response to our get_events request
                    if data.get("id") == 2 and "events" in data:
                        all_events = data["events"]
                        nme_markets = [event for event in all_events if not event.get("mutually_exclusive", True)]
                        print("\n" + "="*40)
                        print("Found the following Non-Mutually Exclusive Markets:")
                        print("="*40)
                        if not nme_markets:
                            print("No open Non-Mutually Exclusive markets were found at this time.")
                        else:
                            for event in nme_markets:
                                print(f"- Title: {event.get('title')}")
                                print(f"  Ticker: {event.get('ticker')}")
                                print(f"  Category: {event.get('category')}")
                                print("-" * 20)
                            print(f"\nTotal NME Market Series Found: {len(nme_markets)}")
                        break  # Exit after printing NME markets
                except Exception as e:
                    print(f"Error processing response: {e}")
            # Optionally, close the websocket after processing
    except websockets.WebSocketException as e:
        print(f"WebSocket connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

asyncio.run(connect_to_websocket())

print(f"Signed Message: {msg_string}")
print(f"Generated Signature: {sig}")