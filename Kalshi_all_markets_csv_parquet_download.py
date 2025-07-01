# export_markets_raw_to_parquet_csv.py

import time
import base64
import json
import requests               # pip install requests
import pandas as pd           # pip install pandas pyarrow fastparquet
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
import os

API_KEY          = ""
PRIVATE_KEY_PATH = r""
REST_HOST        = "https://api.elections.kalshi.com"
PAGE_SIZE        = 500

def load_private_key(path):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

_priv_key = load_private_key(PRIVATE_KEY_PATH)

def build_headers(path: str) -> dict:
    ts      = str(int(time.time() * 1000))
    payload = ts + "GET" + path
    sig     = _priv_key.sign(
        payload.encode("utf-8"),
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY":       API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("ascii")
    }

def export_all_markets_jsonl(filename="markets.jsonl"):
    endpoint = "/trade-api/v2/markets"
    url      = REST_HOST + endpoint

    cursor = None
    total  = 0
    with open(filename, "w", encoding="utf-8") as fout:
        print(f"Dumping markets to {filename}", flush=True)
        while True:
            params = {"limit": PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(url, headers=build_headers(endpoint), params=params)
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("markets", [])

            for m in markets:
                fout.write(json.dumps(m, ensure_ascii=False) + "\n")
            total += len(markets)
            print(f"Wrote page: {len(markets)} markets (total so far: {total})", flush=True)

            cursor = data.get("cursor")
            if not cursor:
                break

    print(f"Done! Wrote {total} markets to {filename}", flush=True)
    return filename

def convert_jsonl_to_parquet_csv(jsonl_file):
    """Convert JSONL file to Parquet and CSV formats using chunked reading"""
    print(f"Converting {jsonl_file} to Parquet and CSV formats...")
    
    # Get file size for progress tracking
    file_size = os.path.getsize(jsonl_file)
    print(f"File size: {file_size / (1024*1024):.2f} MB")
    
    # Read in chunks to avoid memory issues
    chunk_size = 10000  # Process 10k lines at a time
    all_chunks = []
    total_lines = 0
    
    print("Reading JSONL file in chunks...")
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        chunk = []
        for i, line in enumerate(f):
            try:
                data = json.loads(line.strip())
                chunk.append(data)
                total_lines += 1
                
                # Process chunk when it reaches the size limit
                if len(chunk) >= chunk_size:
                    chunk_df = pd.DataFrame(chunk)
                    all_chunks.append(chunk_df)
                    chunk = []
                    print(f"Processed {total_lines:,} lines so far...")
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing line {i+1}: {e}")
                continue
        
        # Process remaining data in the last chunk
        if chunk:
            chunk_df = pd.DataFrame(chunk)
            all_chunks.append(chunk_df)
            total_lines += len(chunk)
    
    print(f"Total lines processed: {total_lines:,}")
    
    # Combine all chunks
    print("Combining chunks...")
    if all_chunks:
        df = pd.concat(all_chunks, ignore_index=True)
        print(f"Final DataFrame shape: {df.shape}")
        
        # Save to Parquet
        parquet_file = jsonl_file.replace('.jsonl', '.parquet')
        print(f"Saving to Parquet: {parquet_file}")
        df.to_parquet(parquet_file, index=False)
        print(f"Saved {len(df):,} markets to {parquet_file}")
        
        # Save to CSV (optional - can be commented out if too large)
        csv_file = jsonl_file.replace('.jsonl', '.csv')
        print(f"Saving to CSV: {csv_file}")
        df.to_csv(csv_file, index=False)
        print(f"Saved {len(df):,} markets to {csv_file}")
        
        # Print some statistics
        print("\nDataFrame Info:")
        print(f"Columns: {list(df.columns)}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
        
        return df
    else:
        print("No valid data found in JSONL file")
        return None

def main():
    jsonl = export_all_markets_jsonl()
    convert_jsonl_to_parquet_csv(jsonl)

if __name__ == "__main__":
    main()
