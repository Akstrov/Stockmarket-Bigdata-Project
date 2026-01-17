from pathlib import Path
import pandas as pd
import json
from kafka import KafkaProducer
from datetime import datetime

# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = (BASE_DIR / "../data").resolve()
TRAIN_DIR = DATA_DIR / "train"

STOCK_TRAIN_PATH = TRAIN_DIR / "stock_train.csv"
REDDIT_TRAIN_PATH = TRAIN_DIR / "reddit_train.csv"

# ------------------------------------------------------------------
# Kafka Producer
# ------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
    max_in_flight_requests_per_connection=1,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def normalize_timestamp(ts):
    """
    Convert Unix or string timestamp to ISO UTC.
    Returns None if invalid.
    """
    try:
        if pd.isna(ts):
            return None
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(float(ts)).isoformat()
        return pd.to_datetime(ts, utc=True).isoformat()
    except:
        return None


# ------------------------------------------------------------------
# Send stock data
# ------------------------------------------------------------------
def send_stock_training():
    print("\n📊 Sending stock training data...")

    df = pd.read_csv(STOCK_TRAIN_PATH)
    print(f"   Rows: {len(df)}")

    sent = 0
    for _, row in df.iterrows():
        msg = {
            "ticker": str(row["ticker"]),
            "date": str(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "source": "training",
        }
        producer.send("stock-data", msg)
        sent += 1

    producer.flush()
    print(f"   ✅ Sent {sent} stock messages")


# ------------------------------------------------------------------
# Send reddit data
# ------------------------------------------------------------------
def send_reddit_training():
    print("\n📱 Sending reddit training data...")

    df = pd.read_csv(REDDIT_TRAIN_PATH)
    print(f"   Rows: {len(df)}")

    text_col = "body" if "body" in df.columns else "selftext"
    comments_col = "num_comments" if "num_comments" in df.columns else "comms_num"

    sent = 0
    skipped = 0

    for _, row in df.iterrows():
        ts = normalize_timestamp(row.get("timestamp", row.get("created_utc")))

        if ts is None:
            skipped += 1
            continue

        msg = {
            "id": str(row["id"]),
            "timestamp": ts,
            "title": str(row["title"]) if pd.notna(row["title"]) else "",
            "body": str(row[text_col]) if pd.notna(row[text_col]) else "",
            "score": int(row["score"]) if pd.notna(row["score"]) else 0,
            "num_comments": int(row[comments_col])
            if pd.notna(row[comments_col])
            else 0,
            "source": "training",
        }

        producer.send("reddit-data", msg)
        sent += 1

    producer.flush()
    print(f"   ✅ Sent {sent} reddit messages")
    print(f"   ⚠️  Skipped {skipped} invalid timestamp rows")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print("SENDING TRAINING DATA TO KAFKA")
    print("=" * 70)

    send_stock_training()
    send_reddit_training()

    producer.close()
    print("\n✅ ALL DATA SENT")
