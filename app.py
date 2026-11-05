"""
Streaming Data Dashboard
Big Data Streaming Dashboard (CoinGecko -> Kafka -> PySpark -> MongoDB Atlas)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

MONGO_URI = "mongodb+srv://ivanrextamondong_db_user:ivanrextamondong_db_password@groceryinventorysystem.lnobtgv.mongodb.net/?retryWrites=true&w=majority"

@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka broker address (e.g., localhost:9092)"
    )
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="crypto_stream",
        help="Kafka topic to consume live CoinGecko stream from"
    )
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["MongoDB", "HDFS"],
        index=0,
        help="Historical data storage sink populated by PySpark Structured Streaming"
    )
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

def generate_sample_data():
    return pd.DataFrame(columns=['timestamp', 'cryptocurrency', 'price_usd', 'volume_24h_usd'])

def consume_kafka_data(config):
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "crypto_stream")
    
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state or st.session_state[cache_key] is None:
        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=5000
                )
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Kafka connection attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(retry_delay)
                else:
                    st.error(f"Failed to connect to Kafka: {e}")
                    st.session_state[cache_key] = None
    
    consumer = st.session_state[cache_key]
    if consumer:
        try:
            messages = []
            start_time = time.time()
            poll_timeout = 5
            while time.time() - start_time < poll_timeout and len(messages) < 50:
                msg_pack = consumer.poll(timeout_ms=1000)
                for tp, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        try:
                            data = message.value
                            if isinstance(data, dict) and all(k in data for k in ['timestamp', 'cryptocurrency', 'price_usd', 'volume_24h_usd']):
                                timestamp_str = str(data['timestamp'])
                                if timestamp_str.endswith('Z'):
                                    timestamp_str = timestamp_str[:-1] + '+00:00'
                                timestamp = datetime.fromisoformat(timestamp_str)
                                messages.append({
                                    'timestamp': timestamp,
                                    'cryptocurrency': str(data['cryptocurrency']),
                                    'price_usd': float(data['price_usd']),
                                    'volume_24h_usd': float(data['volume_24h_usd'])
                                })
                        except (ValueError, KeyError, TypeError) as e:
                            st.warning(f"Error processing message: {e}")
            if messages:
                return pd.DataFrame(messages)
            return generate_sample_data()
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            st.error(f"Kafka error: {e}.")
            return generate_sample_data()
    else:
        st.error("Unable to connect to Kafka. Please verify the broker address.")
        return generate_sample_data()

def query_historical_data(time_range="1h", metric_type="price_usd", coin_filter="all", aggregation="raw"):
    try:
        client = get_mongo_client()
        collection = client["crypto_db"]["historical_data"]
        
        mongo_query = {
            "timestamp": {"$ne": None},
            "cryptocurrency": {"$ne": None},
            "price_usd": {"$ne": None}
        }
        if coin_filter and coin_filter != "all":
            mongo_query["cryptocurrency"] = coin_filter
            
        cursor = collection.find(mongo_query, {"_id": 0}).sort("timestamp", -1).limit(5000)
        records = list(cursor)
        if not records:
            return generate_sample_data()
            
        df = pd.DataFrame(records)
        expected_cols = ['timestamp', 'cryptocurrency', 'price_usd', 'volume_24h_usd']
        for col_name in expected_cols:
            if col_name not in df.columns:
                df[col_name] = None
                
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
        df['volume_24h_usd'] = pd.to_numeric(df['volume_24h_usd'], errors='coerce')
        df = df.dropna(subset=['timestamp', 'cryptocurrency', 'price_usd'])
        
        if df.empty:
            return generate_sample_data()
            
        max_ts = df['timestamp'].max()
        time_deltas = {
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30)
        }
        if time_range in time_deltas and pd.notna(max_ts):
            cutoff = max_ts - time_deltas[time_range]
            filtered_df = df[df['timestamp'] >= cutoff]
            if not filtered_df.empty:
                df = filtered_df
                
        resample_map = {"hourly": "1h", "daily": "1D", "weekly": "1W"}
        if aggregation in resample_map:
            rule = resample_map[aggregation]
            df = (
                df.set_index('timestamp')
                .groupby('cryptocurrency')[['price_usd', 'volume_24h_usd']]
                .resample(rule)
                .mean()
                .reset_index()
                .dropna(subset=['price_usd'])
            )
            
        return df[['timestamp', 'cryptocurrency', 'price_usd', 'volume_24h_usd']].sort_values('timestamp', ascending=False)
    except Exception as e:
        st.error(f"Error querying MongoDB Atlas: {e}")
        return generate_sample_data()

def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")
    
    if 'live_data_buffer' not in st.session_state:
        st.session_state.live_data_buffer = generate_sample_data()
    elif not st.session_state.live_data_buffer.empty and 'price_usd' not in st.session_state.live_data_buffer.columns:
        st.session_state.live_data_buffer = generate_sample_data()
        
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    
    if real_time_data is not None:
        if not real_time_data.empty:
            st.session_state.refresh_state['last_refresh'] = datetime.now()
            st.session_state.live_data_buffer = (
                pd.concat([st.session_state.live_data_buffer, real_time_data], ignore_index=True)
                .drop_duplicates(subset=['timestamp', 'cryptocurrency'])
                .tail(500)
            )
            
        data_freshness = datetime.now() - st.session_state.refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 15 else "🟡" if data_freshness.total_seconds() < 45 else "🔴"
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        if not st.session_state.live_data_buffer.empty:
            buffer_data = st.session_state.live_data_buffer
            
            st.subheader("📊 Live Data Metrics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records in Buffer", len(buffer_data))
            with col2:
                latest_price = buffer_data['price_usd'].iloc[-1]
                latest_coin = buffer_data['cryptocurrency'].iloc[-1]
                st.metric(f"Latest Price ({latest_coin.capitalize()})", f"${latest_price:,.2f}")
            with col3:
                st.metric("Data Range", f"{buffer_data['timestamp'].min().strftime('%H:%M:%S')} - {buffer_data['timestamp'].max().strftime('%H:%M:%S')}")
            
            st.subheader("📈 Real-time Trend")
            all_coins = sorted(buffer_data['cryptocurrency'].unique().tolist())
            f_col1, f_col2 = st.columns([3, 1])
            with f_col1:
                selected_coins = st.multiselect(
                    "Filter Cryptocurrencies",
                    options=all_coins,
                    default=all_coins,
                    key="rt_coin_filter"
                )
            with f_col2:
                use_log = st.checkbox("Logarithmic Y-Axis", value=True, key="rt_log_scale")
            
            filtered_price_data = buffer_data[buffer_data['cryptocurrency'].isin(selected_coins)]
            fig = px.line(
                filtered_price_data,
                x='timestamp',
                y='price_usd',
                color='cryptocurrency',
                markers=True,
                title="Real-time Crypto Prices",
                labels={'price_usd': 'Price (USD)', 'timestamp': 'Time', 'cryptocurrency': 'Cryptocurrency'}
            )
            if use_log:
                fig.update_yaxes(type="log")
            fig.update_layout(xaxis_title="Time", yaxis_title="Price (USD)", hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("📋 View Raw Data (Includes Volume)"):
                display_df = buffer_data.sort_values('timestamp', ascending=False)
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=300)
        else:
            st.warning("Awaiting live data from Kafka... Please ensure producer.py is running.")

def display_historical_view(config):
    st.header("📊 Historical Data Analysis (MongoDB Atlas via PySpark)")
    st.subheader("Data Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        time_range = st.selectbox("Time Range", ["1h", "24h", "7d", "30d"])
    with col2:
        metric_type = st.selectbox("Metric Type", ["price_usd", "volume_24h_usd"])
    with col3:
        coin_filter = st.selectbox("Cryptocurrency", ["all", "uniswap", "toncoin", "polkadot", "near", 
                "render-token", "cosmos", "filecoin", "sui", 
                "optimism", "lido-dao"])
    with col4:
        aggregation = st.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])
    
    historical_data = query_historical_data(
        time_range=time_range,
        metric_type=metric_type,
        coin_filter=coin_filter,
        aggregation=aggregation
    )
    
    if historical_data is not None and not historical_data.empty:
        st.subheader("Historical Data Table")
        st.dataframe(historical_data, use_container_width=True, hide_index=True, height=300)
        
        st.subheader("Historical Trends")
        use_hist_log = st.checkbox("Logarithmic Y-Axis (Historical)", value=(coin_filter == "all"), key="hist_log_scale")
        
        y_col = "volume_24h_usd" if metric_type == "volume_24h_usd" else "price_usd"
        y_label = "Volume (24h USD)" if metric_type == "volume_24h_usd" else "Price (USD)"
        
        fig = px.line(
            historical_data.sort_values('timestamp'),
            x='timestamp',
            y=y_col,
            color='cryptocurrency',
            markers=True,
            title=f"Historical Trend Analysis ({y_label})",
            labels={y_col: y_label, 'timestamp': 'Time', 'cryptocurrency': 'Cryptocurrency'}
        )
        if use_hist_log:
            fig.update_yaxes(type="log")
        fig.update_layout(hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Data Summary")
        s_col1, s_col2 = st.columns(2)
        with s_col1:
            st.metric("Total Historical Records Matched", len(historical_data))
            st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')}")
        with s_col2:
            st.metric(f"Average {y_label}", f"${historical_data[y_col].mean():,.2f}")
            val_std = historical_data[y_col].std()
            st.metric("Data Variability (Std Dev)", f"${0.0 if pd.isna(val_std) else val_std:,.2f}")
    else:
        st.info("No historical records matched the selected filters yet. Ensure historical_consumer.py is running.")

def main():
    st.title("🚀 Streaming Data Dashboard")
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'last_refresh': datetime.now(), 'auto_refresh': True}
    
    config = setup_sidebar()
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox("Enable Auto Refresh", value=st.session_state.refresh_state['auto_refresh'])
    
    refresh_interval = 15
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
