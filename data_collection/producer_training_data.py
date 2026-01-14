from pathlib import Path
import pandas as pd
import json
from kafka import KafkaProducer
from datetime import datetime

# ------------------------------------------------------------------
# Path handling (script-relative)
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = (BASE_DIR / "../data").resolve()

TRAIN_DIR = DATA_DIR / "train"

STOCK_TRAIN_PATH = TRAIN_DIR / "stock_train.csv"
REDDIT_TRAIN_PATH = TRAIN_DIR / "reddit_train.csv"

# ------------------------------------------------------------------
# Initialize Kafka producer
# ------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),  # Handle dates
    acks='all',
    retries=3,
    max_in_flight_requests_per_connection=1  # Ensure ordering
)

# ------------------------------------------------------------------
# Functions to send data
# ------------------------------------------------------------------
def send_stock_training():
    """Send ALL stock training data to Kafka"""
    print("\n📊 Sending stock training data...")
    if not STOCK_TRAIN_PATH.exists():
        raise FileNotFoundError(f"{STOCK_TRAIN_PATH} not found!")

    df = pd.read_csv(STOCK_TRAIN_PATH)
    
    print(f"   Columns: {df.columns.tolist()}")
    print(f"   Rows: {len(df)}")
    
    sent = 0
    for _, row in df.iterrows():
        try:
            message = {
                'ticker': str(row['ticker']),
                'date': str(row['date']),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']),
                'source': 'training'
            }
            
            producer.send('stock-data', message)
            sent += 1
            
            if sent % 100 == 0:
                print(f"   Sent {sent}/{len(df)} stock messages...")
            
        except Exception as e:
            print(f"   ⚠️  Skipped row: {e}")
            continue
    
    producer.flush()
    print(f"   ✅ Sent {sent} stock messages")

def send_reddit_training():
    """Send ALL reddit training data to Kafka"""
    print("\n📱 Sending reddit training data...")
    if not REDDIT_TRAIN_PATH.exists():
        raise FileNotFoundError(f"{REDDIT_TRAIN_PATH} not found!")

    df = pd.read_csv(REDDIT_TRAIN_PATH)
    
    print(f"   Columns: {df.columns.tolist()}")
    print(f"   Rows: {len(df)}")
    
    # Detect text column
    text_col = None
    if 'body' in df.columns:
        text_col = 'body'
    elif 'selftext' in df.columns:
        text_col = 'selftext'
    elif 'text' in df.columns:
        text_col = 'text'
    else:
        print(f"   ⚠️  Warning: No text column found")
        text_col = 'body'  # fallback
    
    # Detect comments column
    comments_col = 'num_comments' if 'num_comments' in df.columns else 'comms_num'
    
    sent = 0
    for idx, row in df.iterrows():
        try:
            message = {
                'id': str(row['id']),
                'timestamp': str(row.get('timestamp', row.get('created_utc', ''))),
                'title': str(row['title']) if pd.notna(row['title']) else "",
                'body': str(row[text_col]) if text_col in df.columns and pd.notna(row[text_col]) else "",
                'score': int(row['score']) if pd.notna(row['score']) else 0,
                'num_comments': int(row[comments_col]) if pd.notna(row[comments_col]) else 0,
                'source': 'training'
            }
            
            producer.send('reddit-data', message)
            sent += 1
            
            if sent % 1000 == 0:
                print(f"   Sent {sent}/{len(df)} reddit messages...")
                
        except Exception as e:
            if sent < 10:  # Only print first 10 errors
                print(f"   ⚠️  Skipped row {idx}: {e}")
            continue
    
    producer.flush()
    print(f"   ✅ Sent {sent} reddit messages")

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    print("="*70)
    print("SENDING TRAINING DATA TO KAFKA")
    print("="*70)
    print(f"Kafka: localhost:29092")
    print(f"Topics: 'stock-data' and 'reddit-data'")
    
    try:
        send_stock_training()
        send_reddit_training()
        
        producer.flush()
        producer.close()
        
        print("\n" + "="*70)
        print("✅ ALL TRAINING DATA SENT TO KAFKA!")
        print("="*70)
        
    except KeyboardInterrupt:
        print("\n⛔ Interrupted by user")
        producer.close()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        producer.close()
