#!/usr/bin/env python3
"""
Dashboard for 2020-2021 WSB Stock Prediction Project
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta
import numpy as np

# Page config
st.set_page_config(page_title="2020-2021 WSB Dashboard", page_icon="📈", layout="wide")

# Title
st.title("📊 2020-2021 WSB Stock Prediction Dashboard")
st.markdown("**GameStop Saga Analysis - January 2021**")


# Connect to MongoDB
@st.cache_resource
def init_mongo():
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        return client
    except Exception as e:
        st.error(f"❌ Cannot connect to MongoDB: {e}")
        return None


client = init_mongo()

if client is None:
    st.stop()

db = client["stockmarket_db"]

# Sidebar
st.sidebar.header("Dashboard Controls")

# Available tickers from your data
TICKERS = ["GME", "AMC", "TSLA", "AAPL", "BB", "NOK", "PLTR", "SPCE"]
selected_ticker = st.sidebar.selectbox("Select Stock", TICKERS, index=0)

# Date range slider - adjusted for 2020-2021
st.sidebar.subheader("Date Range")
use_full_range = st.sidebar.checkbox("Show All Data (2020-2021)", value=True)

if not use_full_range:
    # Show specific month during GME saga
    months = [
        "Jan 2021",
        "Dec 2020",
        "Nov 2020",
        "Oct 2020",
        "Sep 2020",
        "All 2020-2021",
    ]
    selected_month = st.sidebar.selectbox("Focus on Month", months, index=0)

    # Map month to date range
    month_ranges = {
        "Jan 2021": ("2021-01-01", "2021-01-31"),
        "Dec 2020": ("2020-12-01", "2020-12-31"),
        "Nov 2020": ("2020-11-01", "2020-11-30"),
        "Oct 2020": ("2020-10-01", "2020-10-31"),
        "Sep 2020": ("2020-09-01", "2020-09-30"),
        "All 2020-2021": ("2020-01-01", "2021-12-31"),
    }


# Load data functions - NO DATE FILTERING
@st.cache_data(ttl=300)
def load_stock_data(ticker):
    """Load ALL stock prices from MongoDB"""
    cursor = db.stock_raw.find({"ticker": ticker}).sort("trade_date", 1)

    data = list(cursor)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Ensure correct types
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    if "date" in df.columns:  # Backup column name
        df["trade_date"] = pd.to_datetime(df["date"])

    # Convert numeric columns
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Calculate daily returns
    if "close" in df.columns:
        df["daily_return"] = df["close"].pct_change() * 100

    # Sort by date
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date")

    return df


@st.cache_data(ttl=300)
def load_reddit_features(ticker):
    """Load ALL aggregated Reddit features"""
    cursor = db.reddit_features_15m.find({"ticker": ticker}).sort("window_start", 1)

    data = list(cursor)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Convert timestamp
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"])

    # Convert numeric fields
    numeric_cols = [
        "post_count",
        "avg_score",
        "total_score",
        "avg_comments",
        "max_score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort by time
    if "window_start" in df.columns:
        df = df.sort_values("window_start")

    return df


@st.cache_data(ttl=300)
def load_recent_posts(ticker, limit=20):
    """Load recent Reddit posts mentioning the ticker"""
    cursor = (
        db.reddit_raw.find({"tickers": {"$in": [ticker]}})
        .sort("event_time", -1)
        .limit(limit)
    )

    data = list(cursor)
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Convert timestamp
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"])

    return df


# Load ALL data
stock_df = load_stock_data(selected_ticker)
reddit_df = load_reddit_features(selected_ticker)
recent_posts = load_recent_posts(selected_ticker, 20)

# Apply date filter if not using full range
if not use_full_range and "selected_month" in locals():
    start_date, end_date = month_ranges[selected_month]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if not stock_df.empty and "trade_date" in stock_df.columns:
        stock_df = stock_df[
            (stock_df["trade_date"] >= start_date)
            & (stock_df["trade_date"] <= end_date)
        ]

    if not reddit_df.empty and "window_start" in reddit_df.columns:
        reddit_df = reddit_df[
            (reddit_df["window_start"] >= start_date)
            & (reddit_df["window_start"] <= end_date)
        ]

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(
    ["📊 Overview", "📈 Stock Analysis", "💬 Reddit Activity", "🔗 Correlation"]
)

with tab1:
    st.header(f"{selected_ticker} - January 2021 Analysis")

    if not stock_df.empty:
        # Show data range
        min_date = (
            stock_df["trade_date"].min()
            if "trade_date" in stock_df.columns
            else "Unknown"
        )
        max_date = (
            stock_df["trade_date"].max()
            if "trade_date" in stock_df.columns
            else "Unknown"
        )
        st.caption(f"Data Range: {min_date.date()} to {max_date.date()}")

        # Key metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if len(stock_df) > 0:
                current_price = (
                    stock_df.iloc[-1]["close"] if "close" in stock_df.columns else 0
                )
                st.metric("Final Price", f"${current_price:.2f}")

        with col2:
            if len(stock_df) > 1:
                start_price = (
                    stock_df.iloc[0]["close"] if "close" in stock_df.columns else 0
                )
                end_price = (
                    stock_df.iloc[-1]["close"] if "close" in stock_df.columns else 0
                )
                total_change = (end_price - start_price) / start_price * 100
                st.metric("Total Return", f"{total_change:+.1f}%")

        with col3:
            total_posts = (
                reddit_df["post_count"].sum()
                if not reddit_df.empty and "post_count" in reddit_df.columns
                else 0
            )
            st.metric("Total Reddit Posts", f"{int(total_posts):,}")

        with col4:
            if not reddit_df.empty and "avg_comments" in reddit_df.columns:
                avg_comments = reddit_df["avg_comments"].mean()
                st.metric("Avg Comments", f"{avg_comments:.1f}")

        # Combined chart
        if not stock_df.empty and not reddit_df.empty:
            fig = go.Figure()

            # Stock price
            fig.add_trace(
                go.Scatter(
                    x=stock_df["trade_date"],
                    y=stock_df["close"],
                    name=f"{selected_ticker} Price",
                    line=dict(color="blue", width=2),
                    yaxis="y1",
                )
            )

            # Reddit posts (aggregated daily)
            reddit_df["date"] = reddit_df["window_start"].dt.date
            reddit_daily = reddit_df.groupby("date")["post_count"].sum().reset_index()
            reddit_daily["date"] = pd.to_datetime(reddit_daily["date"])

            if not reddit_daily.empty:
                # Scale for visualization
                max_price = stock_df["close"].max()
                max_posts = reddit_daily["post_count"].max()
                if max_posts > 0:
                    scale_factor = max_price / max_posts * 0.5

                    fig.add_trace(
                        go.Bar(
                            x=reddit_daily["date"],
                            y=reddit_daily["post_count"] * scale_factor,
                            name="Reddit Posts (scaled)",
                            marker_color="rgba(255, 99, 132, 0.6)",
                            yaxis="y1",
                        )
                    )

            fig.update_layout(
                title=f"{selected_ticker}: Stock Price vs Reddit Activity",
                xaxis_title="Date",
                yaxis_title="Price ($)",
                height=500,
                hovermode="x unified",
            )

            st.plotly_chart(fig, use_container_width=True)
        elif not stock_df.empty:
            # Just show stock price if no reddit data
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=stock_df["trade_date"],
                    y=stock_df["close"],
                    name=f"{selected_ticker} Price",
                    line=dict(color="blue", width=2),
                )
            )
            fig.update_layout(title=f"{selected_ticker} Stock Price", height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Recent posts section
        st.subheader("📢 Sample Reddit Posts from 2021")
        if not recent_posts.empty:
            # Filter for 2021 posts
            recent_posts["year"] = recent_posts["event_time"].dt.year
            posts_2021 = recent_posts[recent_posts["year"] == 2021].head(3)

            if not posts_2021.empty:
                for _, post in posts_2021.iterrows():
                    with st.expander(
                        f"{post['event_time'].strftime('%Y-%m-%d %H:%M')} - Score: {post.get('score', 0)}"
                    ):
                        st.write(f"**{post.get('title', 'No title')}**")
                        if post.get("body"):
                            st.write(post["body"][:200] + "...")
                        st.caption(
                            f"Comments: {post.get('num_comments', 0)} | Tickers: {post.get('tickers', [])}"
                        )
            else:
                # Show any posts if no 2021 posts
                for _, post in recent_posts.head(3).iterrows():
                    with st.expander(
                        f"{post['event_time'].strftime('%Y-%m-%d %H:%M')} - Score: {post.get('score', 0)}"
                    ):
                        st.write(f"**{post.get('title', 'No title')}**")
                        st.caption(f"Comments: {post.get('num_comments', 0)}")
        else:
            st.info("No Reddit posts found in database")
    else:
        st.warning(f"No stock data found for {selected_ticker}")

with tab2:
    st.header("Stock Price Analysis")

    if not stock_df.empty:
        # Show date range
        st.caption(f"Showing {len(stock_df)} trading days")

        # Price chart
        fig = go.Figure()

        fig.add_trace(
            go.Candlestick(
                x=stock_df["trade_date"],
                open=stock_df["open"],
                high=stock_df["high"],
                low=stock_df["low"],
                close=stock_df["close"],
                name="OHLC",
            )
        )

        fig.update_layout(
            title=f"{selected_ticker} Stock Price (Candlestick)",
            yaxis_title="Price ($)",
            xaxis_title="Date",
            height=500,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            max_price = stock_df["close"].max()
            st.metric("All-time High", f"${max_price:.2f}")

        with col2:
            min_price = stock_df["close"].min()
            st.metric("All-time Low", f"${min_price:.2f}")

        with col3:
            avg_volume = (
                stock_df["volume"].mean() if "volume" in stock_df.columns else 0
            )
            st.metric("Avg Volume", f"{int(avg_volume):,}")

        with col4:
            if "daily_return" in stock_df.columns:
                volatility = stock_df["daily_return"].std()
                st.metric("Volatility", f"{volatility:.2f}%")
    else:
        st.info("No stock data available")

with tab3:
    st.header("Reddit Activity Analysis")

    if not reddit_df.empty:
        st.caption(f"Showing {len(reddit_df)} 15-minute intervals")

        # Reddit metrics over time
        col1, col2 = st.columns(2)

        with col1:
            fig1 = go.Figure()
            fig1.add_trace(
                go.Scatter(
                    x=reddit_df["window_start"],
                    y=reddit_df["post_count"],
                    mode="lines",
                    name="Posts",
                    line=dict(color="purple"),
                )
            )
            fig1.update_layout(
                title="Reddit Post Frequency",
                xaxis_title="Time",
                yaxis_title="Posts per 15min",
                height=300,
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = go.Figure()
            fig2.add_trace(
                go.Scatter(
                    x=reddit_df["window_start"],
                    y=reddit_df["avg_score"],
                    mode="lines",
                    name="Avg Score",
                    line=dict(color="orange"),
                )
            )
            fig2.update_layout(
                title="Post Engagement (Avg Score)",
                xaxis_title="Time",
                yaxis_title="Score",
                height=300,
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Reddit statistics
        st.subheader("Reddit Statistics")

        if not reddit_df.empty:
            metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)

            with metrics_col1:
                total_posts = reddit_df["post_count"].sum()
                st.metric("Total Posts", f"{int(total_posts):,}")

            with metrics_col2:
                peak_posts = reddit_df["post_count"].max()
                st.metric("Peak Posts (15min)", f"{int(peak_posts)}")

            with metrics_col3:
                peak_time = reddit_df.loc[
                    reddit_df["post_count"].idxmax(), "window_start"
                ]
                st.metric("Peak Time", peak_time.strftime("%m/%d %H:%M"))

            with metrics_col4:
                total_score = (
                    reddit_df["total_score"].sum()
                    if "total_score" in reddit_df.columns
                    else 0
                )
                st.metric("Total Score", f"{int(total_score):,}")
    else:
        st.warning(f"No Reddit data found for {selected_ticker}")

with tab4:
    st.header("Correlation Analysis")

    if not stock_df.empty and not reddit_df.empty:
        # Merge stock and reddit data by date
        reddit_df["date"] = reddit_df["window_start"].dt.date
        reddit_daily = (
            reddit_df.groupby("date")
            .agg({"post_count": "sum", "avg_score": "mean"})
            .reset_index()
        )

        stock_df["date"] = stock_df["trade_date"].dt.date

        # Merge
        merged_df = pd.merge(
            stock_df[["date", "close", "daily_return"]],
            reddit_daily,
            on="date",
            how="inner",
        )

        if not merged_df.empty and len(merged_df) > 5:
            # Calculate correlation
            correlation = merged_df["post_count"].corr(merged_df["daily_return"])

            st.subheader(f"Correlation: Reddit Posts vs Stock Returns")
            st.metric(
                "Correlation Coefficient",
                f"{correlation:.3f}",
                delta="Positive" if correlation > 0 else "Negative",
                delta_color="normal",
            )

            # Interpretation
            if correlation > 0.3:
                st.success(
                    "✅ Strong positive correlation: More Reddit posts → Higher returns"
                )
            elif correlation > 0.1:
                st.info("📊 Moderate positive correlation")
            elif correlation > -0.1:
                st.warning("🤔 Weak correlation")
            elif correlation > -0.3:
                st.info("📊 Moderate negative correlation")
            else:
                st.error(
                    "⚠️ Strong negative correlation: More Reddit posts → Lower returns"
                )

            # Scatter plot
            fig = px.scatter(
                merged_df,
                x="post_count",
                y="daily_return",
                trendline="ols",
                title=f"Reddit Posts vs Stock Returns for {selected_ticker}",
                labels={
                    "post_count": "Daily Reddit Posts",
                    "daily_return": "Daily Return %",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

            # Show raw correlation table
            st.subheader("Correlation Matrix")
            corr_matrix = merged_df[["daily_return", "post_count", "avg_score"]].corr()
            st.dataframe(
                corr_matrix.style.background_gradient(cmap="coolwarm", vmin=-1, vmax=1)
            )
        else:
            st.info("Not enough overlapping data points for correlation analysis")
    else:
        st.warning("Need both stock and Reddit data for correlation analysis")

# System status
st.sidebar.markdown("---")
st.sidebar.header("Data Summary")

if not stock_df.empty:
    st.sidebar.success(f"✅ Stock Data: {len(stock_df)} records")
    if "trade_date" in stock_df.columns:
        st.sidebar.caption(f"From: {stock_df['trade_date'].min().date()}")
        st.sidebar.caption(f"To: {stock_df['trade_date'].max().date()}")
else:
    st.sidebar.error("❌ No Stock Data")

if not reddit_df.empty:
    st.sidebar.success(f"✅ Reddit Data: {len(reddit_df)} intervals")
    if "window_start" in reddit_df.columns:
        st.sidebar.caption(f"From: {reddit_df['window_start'].min().date()}")
else:
    st.sidebar.warning("⚠️ No Reddit Data")

# Refresh button
if st.sidebar.button("🔄 Refresh All Data", type="primary"):
    st.cache_data.clear()
    st.rerun()

# Information box
st.sidebar.markdown("---")
with st.sidebar.expander("ℹ️ About this Data"):
    st.write("""
    **Dataset Info:**
    - Stock data: 2020-2021 period
    - Reddit data: WallStreetBets posts during GameStop saga
    - Focus: January 2021 short squeeze event
    
    **Collections in MongoDB:**
    - `stock_raw`: Daily stock prices
    - `reddit_raw`: Individual Reddit posts
    - `reddit_features_15m`: Aggregated 15-min Reddit metrics
    """)

# Footer
st.sidebar.markdown("---")
st.sidebar.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
st.sidebar.caption("Dashboard v1.0 | 2020-2021 Data")

# Run with auto-refresh every 2 minutes
st.balloons()
