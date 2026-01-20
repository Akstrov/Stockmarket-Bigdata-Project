#!/usr/bin/env python3
"""
Complete Dashboard for Stock Prediction Project
Includes: Overview, Stock Analysis, Reddit Activity, Correlation, and Predictions
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta
import subprocess
import time
from streamlit_autorefresh import st_autorefresh

# ----------------- Page Config -----------------
st.set_page_config(
    page_title="Stock Prediction Dashboard", page_icon="📈", layout="wide"
)

# ----------------------------
# Auto-refresh setup with state preservation
# ----------------------------
# Only refresh every 10 seconds
refresh_interval = 10_000  # 10 seconds in milliseconds

# Initialize session state for active tab
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0

# Auto-refresh with longer interval
count = st_autorefresh(interval=refresh_interval, key="auto_refresh")

st.title("📊 Stock Prediction Dashboard")
st.markdown("Real-time monitoring of stock predictions based on Reddit sentiment")


# ----------------- MongoDB -----------------
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
collections = db.list_collection_names()

# ----------------- Sidebar -----------------
st.sidebar.header("Dashboard Controls")
TICKERS = ["GME", "AMC", "TSLA", "AAPL", "BB", "NOK", "PLTR", "SPCE"]
selected_ticker = st.sidebar.selectbox("Select Stock", TICKERS, index=0)
has_predictions = "predictions" in collections


# ----------------- Load Data -----------------
@st.cache_data(ttl=10)  # Reduced TTL to 10 seconds for faster updates
def load_stock_data(ticker):
    if "stock_raw" not in collections:
        return pd.DataFrame()
    cursor = db.stock_raw.find({"ticker": ticker}).sort("trade_date", 1)
    data = list(cursor)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Ensure trade_date
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    elif "date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["date"])
    # Convert numeric
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "close" in df.columns:
        df["daily_return"] = df["close"].pct_change() * 100
    df = df.sort_values("trade_date")
    return df


@st.cache_data(ttl=10)
def load_reddit_features(ticker):
    if "reddit_features_15m" not in collections:
        return pd.DataFrame()
    cursor = db.reddit_features_15m.find({"ticker": ticker}).sort("window_start", 1)
    data = list(cursor)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["window_start"] = pd.to_datetime(df["window_start"])
    for col in ["post_count", "avg_score", "total_score", "avg_comments", "max_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=10)
def load_predictions(ticker):
    if "predictions" not in collections:
        return pd.DataFrame()
    cursor = db.predictions.find({"ticker": ticker}).sort("timestamp", 1)
    data = list(cursor)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


@st.cache_data(ttl=10)
def load_recent_posts(ticker, limit=10):
    if "reddit_raw" not in collections:
        return pd.DataFrame()
    cursor = (
        db.reddit_raw.find({"tickers": {"$in": [ticker]}})
        .sort("event_time", -1)
        .limit(limit)
    )
    data = list(cursor)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"])
    return df


stock_df = load_stock_data(selected_ticker)
reddit_df = load_reddit_features(selected_ticker)
pred_df = load_predictions(selected_ticker)
recent_posts = load_recent_posts(selected_ticker, 10)

# ----------------- Tabs with state management -----------------
tab_names = ["📊 Overview", "📈 Stock Analysis", "💬 Reddit Activity", "🔗 Correlation"]
if has_predictions or not pred_df.empty:
    tab_names.append("🤖 Predictions")

# Create tabs and track active tab
tabs = st.tabs(tab_names)


# Function to handle tab content rendering
def render_tab_content(tab_index, tab_container):
    with tab_container:
        # ----------------- Overview -----------------
        if tab_index == 0:
            st.header(f"{selected_ticker} - Overview")
            if not stock_df.empty:
                min_date = stock_df["trade_date"].min()
                max_date = stock_df["trade_date"].max()
                st.caption(f"Data Range: {min_date.date()} to {max_date.date()}")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Current Price", f"${stock_df.iloc[-1]['close']:.2f}")
                with col2:
                    if len(stock_df) > 1:
                        price_change = (
                            (stock_df.iloc[-1]["close"] - stock_df.iloc[-2]["close"])
                            / stock_df.iloc[-2]["close"]
                            * 100
                        )
                        st.metric("Daily Change", f"{price_change:+.2f}%")
                with col3:
                    total_posts = (
                        reddit_df["post_count"].sum() if not reddit_df.empty else 0
                    )
                    st.metric("Total Reddit Posts", f"{int(total_posts):,}")
                with col4:
                    if not pred_df.empty and "prediction_pct_error" in pred_df.columns:
                        avg_error = pred_df["prediction_pct_error"].abs().mean()
                        st.metric("Prediction Accuracy", f"{100 - avg_error:.1f}%")
                    else:
                        st.metric("Predictions", "Not Available")
                # Price + Reddit chart
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=stock_df["trade_date"],
                        y=stock_df["close"],
                        name="Price",
                        line=dict(color="#1f77b4"),
                    )
                )
                if not pred_df.empty and "predicted_price" in pred_df.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=pred_df["timestamp"],
                            y=pred_df["predicted_price"],
                            name="Predicted",
                            line=dict(color="#ff7f0e", dash="dash"),
                        )
                    )
                if not reddit_df.empty:
                    reddit_df["date"] = reddit_df["window_start"].dt.date
                    reddit_daily = (
                        reddit_df.groupby("date")["post_count"].sum().reset_index()
                    )
                    reddit_daily["date"] = pd.to_datetime(reddit_daily["date"])
                    max_price = stock_df["close"].max()
                    max_posts = reddit_daily["post_count"].max()
                    scale_factor = max_price / max_posts * 0.3 if max_posts > 0 else 1
                    fig.add_trace(
                        go.Bar(
                            x=reddit_daily["date"],
                            y=reddit_daily["post_count"] * scale_factor,
                            name="Reddit Posts (scaled)",
                            marker_color="#9467bd",
                            yaxis="y1",
                        )
                    )
                fig.update_layout(
                    title=f"{selected_ticker}: Price & Activity",
                    xaxis_title="Date",
                    yaxis_title="Price ($)",
                    hovermode="x unified",
                    height=500,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No stock data found for {selected_ticker}")

        # ----------------- Stock Analysis -----------------
        elif tab_index == 1:
            st.header("Stock Price Analysis")
            if not stock_df.empty:
                subtab1, subtab2, subtab3 = st.tabs(
                    ["Price Chart", "Volume", "Returns"]
                )
                with subtab1:
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
                        title=f"{selected_ticker} Stock Price",
                        yaxis_title="Price ($)",
                        xaxis_title="Date",
                        height=500,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                with subtab2:
                    fig = go.Figure()
                    fig.add_trace(
                        go.Bar(
                            x=stock_df["trade_date"],
                            y=stock_df["volume"],
                            marker_color="#2ca02c",
                        )
                    )
                    fig.update_layout(
                        title="Trading Volume",
                        yaxis_title="Volume",
                        xaxis_title="Date",
                        height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                with subtab3:
                    if "daily_return" in stock_df.columns:
                        colors = [
                            "red" if x < 0 else "green"
                            for x in stock_df["daily_return"]
                        ]
                        fig = go.Figure()
                        fig.add_trace(
                            go.Bar(
                                x=stock_df["trade_date"],
                                y=stock_df["daily_return"],
                                marker_color=colors,
                            )
                        )
                        fig.add_hline(y=0, line_dash="dash", line_color="gray")
                        fig.update_layout(
                            title="Daily Returns (%)",
                            yaxis_title="Return %",
                            xaxis_title="Date",
                            height=400,
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No stock data available")

        # ----------------- Reddit Activity -----------------
        elif tab_index == 2:
            st.header("Reddit Activity Analysis")
            if not reddit_df.empty:
                col1, col2 = st.columns(2)
                fig1 = go.Figure()
                fig1.add_trace(
                    go.Scatter(
                        x=reddit_df["window_start"],
                        y=reddit_df["post_count"],
                        mode="lines",
                        line=dict(color="#9467bd"),
                        name="Posts",
                    )
                )
                fig1.update_layout(
                    title="Reddit Post Frequency",
                    xaxis_title="Time",
                    yaxis_title="Posts/15min",
                    height=300,
                )
                st.plotly_chart(fig1, use_container_width=True)
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Scatter(
                        x=reddit_df["window_start"],
                        y=reddit_df["avg_score"],
                        mode="lines",
                        line=dict(color="#ff7f0e"),
                        name="Avg Score",
                    )
                )
                fig2.update_layout(
                    title="Post Engagement (Avg Score)",
                    xaxis_title="Time",
                    yaxis_title="Score",
                    height=300,
                )
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.warning("No Reddit data found for this stock")

        # ----------------- Correlation -----------------
        elif tab_index == 3:
            st.header("Reddit vs Stock Correlation")
            if stock_df.empty or reddit_df.empty:
                st.info("Not enough data for correlation")
            else:
                s = stock_df[["trade_date", "close"]].copy().sort_values("trade_date")
                s["return"] = s["close"].pct_change()
                r = reddit_df.copy()
                r["date"] = r["window_start"].dt.date
                r_daily = (
                    r.groupby("date")
                    .agg(
                        {"post_count": "sum", "avg_score": "mean", "total_score": "sum"}
                    )
                    .reset_index()
                )
                s["date"] = s["trade_date"].dt.date
                merged = pd.merge(
                    s[["date", "return"]], r_daily, on="date", how="inner"
                ).dropna()
                if len(merged) < 5:
                    st.info("Not enough overlapping days")
                else:
                    corr = merged[
                        ["return", "post_count", "avg_score", "total_score"]
                    ].corr()
                    fig = px.imshow(
                        corr,
                        text_auto=".2f",
                        color_continuous_scale="RdBu",
                        origin="lower",
                        title="Correlation Matrix",
                    )
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(
                        "Red = negative correlation, Blue = positive correlation"
                    )

        # ----------------- Predictions -----------------
        elif tab_index == 4 and (has_predictions or not pred_df.empty):
            st.header(f"{selected_ticker} - Predictions Analysis")
            if pred_df.empty:
                st.warning("No predictions available")
            else:
                # Error over time
                st.subheader("Prediction Error Over Time")
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=pred_df["timestamp"],
                        y=pred_df["prediction_pct_error"],
                        mode="lines+markers",
                        line=dict(color="#d62728"),
                        name="Prediction Error (%)",
                    )
                )
                fig.add_hline(y=0, line_dash="dash", line_color="gray")
                fig.update_layout(
                    title="Prediction Error (%)",
                    yaxis_title="Error %",
                    xaxis_title="Date",
                    height=400,
                )
                st.plotly_chart(fig, use_container_width=True)

                # Direction accuracy
                st.subheader("Direction Accuracy")
                pred_df["actual_dir"] = np.sign(pred_df["actual_price"].diff())
                pred_df["pred_dir"] = np.sign(pred_df["predicted_price"].diff())
                direction_acc = (
                    pred_df["actual_dir"] == pred_df["pred_dir"]
                ).mean() * 100
                st.metric("Correct Direction %", f"{direction_acc:.1f}%")

                # Best/Worst predictions
                st.subheader("🏆 Best & Worst Predictions")
                best = pred_df.nsmallest(5, "prediction_pct_error")[
                    [
                        "timestamp",
                        "actual_price",
                        "predicted_price",
                        "prediction_pct_error",
                    ]
                ]
                worst = pred_df.nlargest(5, "prediction_pct_error")[
                    [
                        "timestamp",
                        "actual_price",
                        "predicted_price",
                        "prediction_pct_error",
                    ]
                ]
                col1, col2 = st.columns(2)
                with col1:
                    st.write("✅ Best Predictions")
                    st.dataframe(
                        best.style.format(
                            {
                                "actual_price": "${:.2f}",
                                "predicted_price": "${:.2f}",
                                "prediction_pct_error": "{:+.2f}%",
                            }
                        )
                    )
                with col2:
                    st.write("❌ Worst Predictions")
                    st.dataframe(
                        worst.style.format(
                            {
                                "actual_price": "${:.2f}",
                                "predicted_price": "${:.2f}",
                                "prediction_pct_error": "{:+.2f}%",
                            }
                        )
                    )


# Render all tabs (this ensures state is preserved)
for i, tab in enumerate(tabs):
    render_tab_content(i, tab)

# ----------------- Sidebar Status -----------------
st.sidebar.markdown("---")
st.sidebar.header("System Status")
st.sidebar.text(f"MongoDB: {'✅ Connected' if client else '❌ Disconnected'}")
st.sidebar.text(f"Stock Data: {'✅ Available' if not stock_df.empty else '❌ Missing'}")
st.sidebar.text(
    f"Reddit Data: {'✅ Available' if not reddit_df.empty else '❌ Missing'}"
)
st.sidebar.text(f"Predictions: {'✅ Available' if not pred_df.empty else '❌ Missing'}")
st.sidebar.text(f"Auto-refresh: Every {refresh_interval / 1000:.0f}s")

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Generate predictions
if st.sidebar.button("🚀 Generate Predictions"):
    st.info("Starting prediction generation...")
    result = subprocess.run(
        ["python", "ml_models/generate_predictions.py"], capture_output=True, text=True
    )
    st.code(result.stdout)
    if result.stderr:
        st.error(result.stderr)
    st.success("Predictions generated! Refresh to see them.")
