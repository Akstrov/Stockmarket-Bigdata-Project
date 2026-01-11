#!/usr/bin/env python3
"""
Verify that both Reddit and Stock datasets are ready
"""
import pandas as pd
import os

def verify_datasets():
    print("="*70)
    print("🔍 VERIFYING DATASETS")
    print("="*70)
    
    # Check stock data
    print("\n1️⃣  Stock Data:")
    stock_file = "data/raw/stock_prices.csv"
    
    if os.path.exists(stock_file):
        df_stock = pd.read_csv(stock_file)
        print(f"   ✅ Found: {stock_file}")
        print(f"   📊 Records: {len(df_stock):,}")
        print(f"   📅 Date range: {df_stock['date'].min()} to {df_stock['date'].max()}")
        print(f"   🏢 Tickers: {sorted(df_stock['ticker'].unique().tolist())}")
        print(f"   📋 Columns: {df_stock.columns.tolist()}")
    else:
        print(f"   ❌ NOT FOUND: {stock_file}")
        print("      Run: python download_stock_stooq.py")
    
    # Check Reddit data
    print("\n2️⃣  Reddit Data:")
    reddit_files = [
        "data/raw/reddit_wsb.csv",
        "data/raw/reddit-wallstreetbets-posts.csv",
        "data/raw/wallstreetbets_posts.csv"
    ]
    
    reddit_file = None
    for file in reddit_files:
        if os.path.exists(file):
            reddit_file = file
            break
    
    if reddit_file:
        df_reddit = pd.read_csv(reddit_file, nrows=1000)  # Just load first 1000 to check
        print(f"   ✅ Found: {reddit_file}")
        print(f"   📋 Columns: {df_reddit.columns.tolist()}")
        
        # Check for required fields
        required = ['title', 'created_utc']
        has_text = 'selftext' in df_reddit.columns or 'body' in df_reddit.columns
        
        missing = [col for col in required if col not in df_reddit.columns]
        
        if missing:
            print(f"   ⚠️  Missing columns: {missing}")
        if has_text:
            print(f"   ✅ Has post text content")
        else:
            print(f"   ⚠️  No 'selftext' or 'body' column found")
        
        print(f"\n   Sample row:")
        print(df_reddit.iloc[0])
        
    else:
        print(f"   ❌ NOT FOUND")
        print("      Download from: https://www.kaggle.com/datasets/gpreda/reddit-wallstreetbets-posts")
        print("      Save to: data/raw/reddit_wsb.csv")
    
    # Summary
    print("\n" + "="*70)
    if os.path.exists(stock_file) and reddit_file:
        print("✅ READY TO PROCEED!")
        print("\nNext step: Prepare data for pipeline")
        print("   Run: python prepare_historical_data.py")
    else:
        print("⚠️  MISSING DATA - Complete downloads first")

if __name__ == "__main__":
    verify_datasets()
