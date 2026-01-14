from pathlib import Path
import pandas as pd

print("=" * 70)
print("SPLITTING DATA INTO TRAIN / SIMULATION SETS")
print("=" * 70)

# ------------------------------------------------------------------
# Path handling (robust, terminal-independent)
# ------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = (BASE_DIR / "../data").resolve()

RAW_DIR = DATA_DIR / "raw"
TRAIN_DIR = DATA_DIR / "train"
SIM_DIR = DATA_DIR / "simulate"

# ------------------------------------------------------------------
# Read data
# ------------------------------------------------------------------

print("\n1. Reading datasets...")

reddit_path = RAW_DIR / "reddit_wsb.csv"
stock_path = RAW_DIR / "stock_prices.csv"

reddit = pd.read_csv(reddit_path)
stock = pd.read_csv(stock_path)

print(f"   Reddit: {len(reddit):,} rows")
print(f"   Stock: {len(stock):,} rows")

# ------------------------------------------------------------------
# Check columns
# ------------------------------------------------------------------

print(f"\n2. Reddit columns: {reddit.columns.tolist()}")
print(f"   Stock columns: {stock.columns.tolist()}")

# ------------------------------------------------------------------
# Convert dates
# ------------------------------------------------------------------

print("\n3. Converting dates...")

if "timestamp" in reddit.columns:
    reddit["date"] = pd.to_datetime(reddit["timestamp"]).dt.date
elif "created_utc" in reddit.columns:
    reddit["date"] = pd.to_datetime(reddit["created_utc"], unit="s").dt.date
else:
    raise ValueError("No timestamp column found in Reddit data")

stock["date"] = pd.to_datetime(stock["date"]).dt.date

# ------------------------------------------------------------------
# Filter to overlapping period
# ------------------------------------------------------------------

print("\n4. Filtering to overlapping period...")

stock_start = stock["date"].min()
stock_end = stock["date"].max()

print(f"   Stock date range: {stock_start} → {stock_end}")

reddit_before = len(reddit)
reddit = reddit[
    (reddit["date"] >= stock_start) &
    (reddit["date"] <= stock_end)
].copy()

print(f"   Reddit filtered: {reddit_before:,} → {len(reddit):,} posts")

# ------------------------------------------------------------------
# Split data
# ------------------------------------------------------------------

split_date = pd.to_datetime("2021-02-28").date()
print(f"\n5. Splitting at: {split_date}")

reddit_train = reddit[reddit["date"] <= split_date].copy()
reddit_sim = reddit[reddit["date"] > split_date].copy()

stock_train = stock[stock["date"] <= split_date].copy()
stock_sim = stock[stock["date"] > split_date].copy()

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------

print("\n" + "=" * 70)
print("SPLIT SUMMARY")
print("=" * 70)

print(f"Training period: {stock_train['date'].min()} → {stock_train['date'].max()}")
print(f"  Reddit: {len(reddit_train):,} posts")
print(f"  Stock: {len(stock_train):,} rows")

print(f"\nSimulation period: {stock_sim['date'].min()} → {stock_sim['date'].max()}")
print(f"  Reddit: {len(reddit_sim):,} posts")
print(f"  Stock: {len(stock_sim):,} rows")

# ------------------------------------------------------------------
# Save results
# ------------------------------------------------------------------

print("\n6. Saving split files...")

TRAIN_DIR.mkdir(parents=True, exist_ok=True)
SIM_DIR.mkdir(parents=True, exist_ok=True)

reddit_train.to_csv(TRAIN_DIR / "reddit_train.csv", index=False)
reddit_sim.to_csv(SIM_DIR / "reddit_sim.csv", index=False)
stock_train.to_csv(TRAIN_DIR / "stock_train.csv", index=False)
stock_sim.to_csv(SIM_DIR / "stock_sim.csv", index=False)

print("✅ All files saved!")
print(f"   Train: {TRAIN_DIR}")
print(f"   Simulate: {SIM_DIR}")
