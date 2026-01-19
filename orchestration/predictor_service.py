# orchestration/simple_predictor.py
#!/usr/bin/env python3
"""
Simple predictor that doesn't try to load broken models
"""

import time
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import numpy as np
import random


class SimplePredictor:
    def __init__(self):
        print("🚀 Simple Predictor - Using Rule-based Predictions")

        # Connect to MongoDB
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client["stockmarket_db"]

        # Simple prediction rules
        self.rules = {
            "momentum": lambda price, posts, score: price
            * (1 + 0.01 if posts > 10 else -0.005),
            "mean_reversion": lambda price, posts, score: price * 0.99
            if price > 10
            else price * 1.01,
            "random_walk": lambda price, posts, score: price
            * (1 + random.uniform(-0.02, 0.02)),
        }

    def predict_for_ticker(self, ticker):
        """Make a simple prediction for a ticker"""
        try:
            # Get latest stock price
            stock = list(
                self.db.stock_raw.find({"ticker": ticker})
                .sort("trade_date", -1)
                .limit(1)
            )
            if not stock:
                return None

            latest_price = stock[0]["close"]
            latest_date = stock[0]["trade_date"]

            # Get recent reddit activity
            reddit_cursor = self.db.reddit_features_15m.find(
                {
                    "ticker": ticker,
                    "window_start": {"$gte": datetime.now() - timedelta(days=2)},
                }
            )

            reddit_data = list(reddit_cursor)

            if reddit_data:
                reddit_df = pd.DataFrame(reddit_data)
                total_posts = reddit_df["post_count"].sum()
                avg_score = reddit_df["avg_score"].mean()
            else:
                total_posts = 0
                avg_score = 0

            # Choose prediction rule based on reddit activity
            if total_posts > 50:
                # High activity - use momentum
                predicted_price = self.rules["momentum"](
                    latest_price, total_posts, avg_score
                )
                rule_used = "momentum"
            elif latest_price > 20:
                # High price - mean reversion
                predicted_price = self.rules["mean_reversion"](
                    latest_price, total_posts, avg_score
                )
                rule_used = "mean_reversion"
            else:
                # Default - random walk
                predicted_price = self.rules["random_walk"](
                    latest_price, total_posts, avg_score
                )
                rule_used = "random_walk"

            # Create prediction record
            prediction = {
                "ticker": ticker,
                "timestamp": datetime.now(),
                "predicted_price": float(predicted_price),
                "current_price": float(latest_price),
                "predicted_change": float(
                    (predicted_price - latest_price) / latest_price * 100
                ),
                "rule_used": rule_used,
                "reddit_posts_last_2_days": int(total_posts),
                "avg_reddit_score": float(avg_score),
                "created_at": datetime.now(),
            }

            print(
                f"📈 {ticker}: ${latest_price:.2f} → ${predicted_price:.2f} ({prediction['predicted_change']:+.2f}%) [{rule_used}]"
            )

            return prediction

        except Exception as e:
            print(f"❌ Error predicting for {ticker}: {e}")
            return None

    def run_once(self):
        """Run one prediction cycle"""
        print("\n" + "=" * 60)
        print(f"🔮 PREDICTION CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        TICKERS = ["GME", "AMC", "TSLA", "AAPL", "BB", "NOK", "PLTR", "SPCE"]

        for ticker in TICKERS:
            prediction = self.predict_for_ticker(ticker)
            if prediction:
                # Store in MongoDB
                self.db.simple_predictions.update_one(
                    {"ticker": ticker}, {"$set": prediction}, upsert=True
                )

        print("\n✅ Simple predictions completed")

    def run_continuous(self, interval=300):
        """Run continuously (for demo purposes)"""
        print("\n" + "=" * 60)
        print("🔮 SIMPLE PREDICTOR - CONTINUOUS MODE")
        print("=" * 60)
        print("Making predictions every 5 minutes")
        print("=" * 60)

        cycle = 0
        try:
            while True:
                cycle += 1
                print(f"\n🔄 Cycle #{cycle}")
                self.run_once()
                print(f"\n⏳ Next prediction in {interval} seconds...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n🛑 Predictor stopped")
        finally:
            self.client.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Simple Predictor")
    parser.add_argument("--mode", choices=["once", "continuous"], default="once")
    parser.add_argument("--interval", type=int, default=300)

    args = parser.parse_args()

    predictor = SimplePredictor()

    if args.mode == "continuous":
        predictor.run_continuous(args.interval)
    else:
        predictor.run_once()
