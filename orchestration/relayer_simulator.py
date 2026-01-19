#!/usr/bin/env python3
"""
Fixed Relayer - Handles CSV parsing issues
"""

import time
import json
import csv
import logging
from datetime import datetime
from kafka import KafkaProducer
import signal
import sys
import os
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FixedRelayer:
    def __init__(self):
        print("\n" + "=" * 60)
        print("🔄 FIXED DATA RELAYER")
        print("=" * 60)

        # Configuration
        self.speed_factor = 36000  # 10 hours per second
        self.running = True

        # Find files
        self.reddit_file = self.find_file("reddit")
        self.stock_file = self.find_file("stock")

        if not self.reddit_file or not self.stock_file:
            print("\n❌ Missing data files!")
            print("   Please run: python debug_csv.py")
            sys.exit(1)

        print(f"\n📁 Using files:")
        print(f"   Reddit: {self.reddit_file}")
        print(f"   Stock:  {self.stock_file}")

        # Initialize Kafka
        self.producer = None
        self.init_kafka()

        # Stats
        self.stats = {"reddit_sent": 0, "stock_sent": 0, "start_time": datetime.now()}

        # Signal handling
        signal.signal(signal.SIGINT, self.signal_handler)

    def find_file(self, file_type):
        """Find CSV file by type"""
        # Check multiple locations
        locations = [
            f"data/raw/{'reddit_wsb' if file_type == 'reddit' else 'stock_prices'}.csv",
            f"data/simulate/{'reddit_sim' if file_type == 'reddit' else 'stock_sim'}.csv",
            f"data/train/{'reddit_train' if file_type == 'reddit' else 'stock_train'}.csv",
        ]

        for location in locations:
            path = Path(location)
            if path.exists():
                return path

        return None

    def init_kafka(self):
        """Initialize Kafka connection"""
        print("\n🌐 Connecting to Kafka...")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers="localhost:29092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: str(v).encode("utf-8"),
                acks="all",
            )
            print("✅ Connected to Kafka")
            return True
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            print("   Make sure Kafka is running: docker-compose up -d kafka")
            return False

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Stopping relayer...")
        self.running = False

    def detect_csv_dialect(self, filepath):
        """Detect CSV dialect (delimiter, quote char, etc.)"""
        try:
            with open(filepath, "r", encoding="utf-8-sig") as f:
                # Read a sample
                sample = f.read(1024 * 10)
                f.seek(0)

                # Try to sniff dialect
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    print(f"   Detected delimiter: '{dialect.delimiter}'")
                    return dialect
                except:
                    # Default to comma
                    print(f"   Using default comma delimiter")
                    return csv.excel  # comma-delimited
        except Exception as e:
            print(f"   Error detecting dialect: {e}")
            return csv.excel

    def stream_reddit_data_simple(self):
        """Stream Reddit data with robust parsing"""
        print(f"\n📤 Streaming Reddit data...")

        try:
            # Detect dialect
            dialect = self.detect_csv_dialect(self.reddit_file)

            with open(self.reddit_file, "r", encoding="utf-8-sig") as file:
                # Read and count total lines first
                total_lines = sum(1 for _ in file) - 1
                file.seek(0)

                reader = csv.DictReader(file, dialect=dialect)

                print(f"   Total records: {total_lines:,}")
                print("   Progress: ", end="", flush=True)

                batch = []
                batch_size = 100

                for i, row in enumerate(reader):
                    if not self.running:
                        break

                    try:
                        # Create message (handle missing columns)
                        message = {
                            "id": row.get("id", str(i)),
                            "timestamp": row.get("timestamp")
                            or row.get("created_utc")
                            or datetime.now().isoformat(),
                            "title": row.get("title", ""),
                            "body": row.get("body", "") or row.get("selftext", ""),
                            "score": self.safe_int(row.get("score", 0)),
                            "num_comments": self.safe_int(row.get("num_comments", 0)),
                            "source": "relayer",
                        }

                        batch.append(message)

                        # Send batch when full
                        if len(batch) >= batch_size:
                            for msg in batch:
                                self.producer.send(
                                    "reddit-data", key=msg["id"], value=msg
                                )
                                self.stats["reddit_sent"] += 1

                            self.producer.flush()
                            batch = []

                            # Print progress
                            if self.stats["reddit_sent"] % 1000 == 0:
                                print(
                                    f"{self.stats['reddit_sent']:,}...",
                                    end="",
                                    flush=True,
                                )

                        # Small delay
                        time.sleep(0.001)

                    except Exception as e:
                        print(f"\n⚠️ Skipping row {i}: {e}")
                        continue

                # Send remaining batch
                if batch:
                    for msg in batch:
                        self.producer.send("reddit-data", key=msg["id"], value=msg)
                        self.stats["reddit_sent"] += 1
                    self.producer.flush()

            print(f"\n   ✅ Sent {self.stats['reddit_sent']:,} Reddit messages")
            return self.stats["reddit_sent"]

        except Exception as e:
            print(f"\n❌ Error streaming Reddit: {e}")
            import traceback

            traceback.print_exc()
            return 0

    def stream_stock_data_simple(self):
        """Stream Stock data with robust parsing"""
        print(f"\n📈 Streaming Stock data...")

        try:
            # Detect dialect
            dialect = self.detect_csv_dialect(self.stock_file)

            with open(self.stock_file, "r", encoding="utf-8-sig") as file:
                # Read and count total lines first
                total_lines = sum(1 for _ in file) - 1
                file.seek(0)

                reader = csv.DictReader(file, dialect=dialect)

                print(f"   Total records: {total_lines:,}")
                print("   Progress: ", end="", flush=True)

                for i, row in enumerate(reader):
                    if not self.running:
                        break

                    try:
                        # Create message (handle missing columns)
                        message = {
                            "ticker": row.get("ticker", "GME"),
                            "date": row.get(
                                "date", datetime.now().strftime("%Y-%m-%d")
                            ),
                            "open": self.safe_float(row.get("open", 0)),
                            "high": self.safe_float(row.get("high", 0)),
                            "low": self.safe_float(row.get("low", 0)),
                            "close": self.safe_float(row.get("close", 0)),
                            "volume": self.safe_int(row.get("volume", 0)),
                            "source": "relayer",
                        }

                        # Send immediately (stock data is less frequent)
                        self.producer.send(
                            "stock-data", key=message["ticker"], value=message
                        )
                        self.stats["stock_sent"] += 1

                        # Print progress
                        if self.stats["stock_sent"] % 10 == 0:
                            print(
                                f"{self.stats['stock_sent']:,}...", end="", flush=True
                            )

                        # Stock data comes daily, so longer delay
                        time.sleep(0.1)

                    except Exception as e:
                        print(f"\n⚠️ Skipping stock row {i}: {e}")
                        continue

                self.producer.flush()

            print(f"\n   ✅ Sent {self.stats['stock_sent']:,} stock records")
            return self.stats["stock_sent"]

        except Exception as e:
            print(f"\n❌ Error streaming Stock: {e}")
            import traceback

            traceback.print_exc()
            return 0

    def safe_int(self, value, default=0):
        """Safely convert to int"""
        try:
            if value is None:
                return default
            return int(float(value))
        except:
            return default

    def safe_float(self, value, default=0.0):
        """Safely convert to float"""
        try:
            if value is None:
                return default
            return float(value)
        except:
            return default

    def run_parallel(self):
        """Run both streams in parallel using threading"""
        print("\n🚀 Starting parallel streaming...")
        print("   Reddit and Stock data will be sent together")
        print("=" * 60)

        import threading

        # Function to run Reddit stream
        def run_reddit():
            self.stream_reddit_data_simple()

        # Function to run Stock stream
        def run_stock():
            self.stream_stock_data_simple()

        # Start threads
        reddit_thread = threading.Thread(target=run_reddit)
        stock_thread = threading.Thread(target=run_stock)

        reddit_thread.start()
        stock_thread.start()

        # Wait for both to complete
        reddit_thread.join()
        stock_thread.join()

        # Print summary
        self.print_summary()

    def run_sequential(self):
        """Run streams sequentially (easier to debug)"""
        print("\n🚀 Starting sequential streaming...")
        print("=" * 60)

        # Stream Reddit first
        print("\n1. Streaming Reddit data:")
        self.stream_reddit_data_simple()

        # Then Stock
        print("\n2. Streaming Stock data:")
        self.stream_stock_data_simple()

        # Print summary
        self.print_summary()

    def print_summary(self):
        """Print streaming summary"""
        runtime = datetime.now() - self.stats["start_time"]
        hours, remainder = divmod(runtime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        print("\n" + "=" * 60)
        print("📊 STREAMING SUMMARY")
        print("=" * 60)
        print(f"Runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print(f"Reddit messages sent: {self.stats['reddit_sent']:,}")
        print(f"Stock records sent: {self.stats['stock_sent']:,}")
        print(
            f"Total messages: {self.stats['reddit_sent'] + self.stats['stock_sent']:,}"
        )
        print("=" * 60)

    def cleanup(self):
        """Clean up resources"""
        if self.producer:
            self.producer.close()
            print("\n✅ Kafka producer closed")
        print("🏁 Relayer shutdown complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fixed Data Relayer")
    parser.add_argument(
        "--mode",
        choices=["parallel", "sequential"],
        default="sequential",
        help="Streaming mode",
    )
    parser.add_argument("--reddit-file", type=str, help="Path to Reddit CSV")
    parser.add_argument("--stock-file", type=str, help="Path to Stock CSV")

    args = parser.parse_args()

    relayer = FixedRelayer()

    # Override file paths if specified
    if args.reddit_file:
        relayer.reddit_file = Path(args.reddit_file)
    if args.stock_file:
        relayer.stock_file = Path(args.stock_file)

    try:
        if not relayer.producer:
            print("❌ Cannot start: Kafka producer not initialized")
            sys.exit(1)

        if args.mode == "parallel":
            relayer.run_parallel()
        else:
            relayer.run_sequential()

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        relayer.cleanup()
