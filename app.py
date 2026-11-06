"""
Streaming Data Dashboard
Big Data Streaming Dashboard (CoinGecko -> Kafka -> PySpark -> MongoDB Atlas)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
import io
from fpdf import FPDF
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Streaming Data Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

MONGO_URI = "mongodb+srv://ivanrextamondong_db_user:ivanrextamondong_db_password@groceryinventorysystem.lnobtgv.mongodb.net/?retryWrites=true&w=majority"

@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    
    with st.sidebar.expander("Data Source Configuration", expanded=True):
        kafka_broker = st.text_input(
            "Kafka Broker",
            value="localhost:9092",
            help="Kafka broker address (e.g., localhost:9092)"
        )
        kafka_topic = st.text_input(
            "Kafka Topic",
            value="crypto_stream",
            help="Kafka topic to consume live CoinGecko stream from"
        )
        
    with st.sidebar.expander("Storage Configuration", expanded=True):
        storage_type = st.selectbox(
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

def generate_text_pdf(df, y_col, y_label):
    pdf = FPDF()
    pdf.add_page()
    
    # Title
    pdf.set_font("Arial", style="B", size=16)
    pdf.cell(200, 10, txt="Cryptocurrency Historical Data Report", ln=True, align='C')
    pdf.ln(5)
    
    # Summary Metrics
    pdf.set_font("Arial", style="B", size=12)
    pdf.cell(200, 10, txt=f"Total Records Queried: {len(df)}", ln=True)
    pdf.cell(200, 10, txt=f"Average {y_label}: ${df[y_col].mean():,.2f}", ln=True)
    val_std = df[y_col].std()
    pdf.cell(200, 10, txt=f"Volatility (Std Dev): ${0.0 if pd.isna(val_std) else val_std:,.2f}", ln=True)
    pdf.ln(5)
    
    # Table Header
    pdf.set_font("Arial", style="B", size=10)
    pdf.cell(50, 10, "Timestamp", border=1, align='C')
    pdf.cell(40, 10, "Cryptocurrency", border=1, align='C')
    pdf.cell(40, 10, "Price (USD)", border=1, align='C')
    pdf.cell(50, 10, "Volume 24h", border=1, align='C', ln=True)
    
    # Table Rows
    pdf.set_font("Arial", size=10)
    for i, row in df.head(100).iterrows():
        ts = str(row['timestamp']).split('+')[0] if pd.notna(row['timestamp']) else ""
        price = float(row['price_usd']) if pd.notna(row['price_usd']) else 0.0
        volume = float(row['volume_24h_usd']) if pd.notna(row['volume_24h_usd']) else 0.0
        
        pdf.cell(50, 10, ts, border=1, align='C')
        pdf.cell(40, 10, str(row['cryptocurrency']).title(), border=1, align='C')
        pdf.cell(40, 10, f"${price:,.2f}", border=1, align='R')
        pdf.cell(50, 10, f"${volume:,.0f}", border=1, align='R', ln=True)
        
    return bytes(pdf.output())

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
                            pass 
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
            
        cursor = collection.find(mongo_query, {"_id": 0}).sort("timestamp", -1).limit(50000)
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
    st.markdown("### Live Streaming Engine")
    
    if 'live_data_buffer' not in st.session_state:
        st.session_state.live_data_buffer = generate_sample_data()
    elif not st.session_state.live_data_buffer.empty and 'price_usd' not in st.session_state.live_data_buffer.columns:
        st.session_state.live_data_buffer = generate_sample_data()
        
    refresh_state = st.session_state.refresh_state
    
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
        freshness_status = "Good" if data_freshness.total_seconds() < 15 else "Warning" if data_freshness.total_seconds() < 45 else "Critical"
        
        st.markdown(f"**Status:** {freshness_status} - Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        if not st.session_state.live_data_buffer.empty:
            buffer_data = st.session_state.live_data_buffer
            
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records in Buffer", len(buffer_data))
            with col2:
                latest_price = buffer_data['price_usd'].iloc[-1]
                latest_coin = buffer_data['cryptocurrency'].iloc[-1]
                st.metric(f"Latest Price ({latest_coin.capitalize()})", f"${latest_price:,.2f}")
            with col3:
                time_range_str = f"{buffer_data['timestamp'].min().strftime('%H:%M:%S')} - {buffer_data['timestamp'].max().strftime('%H:%M:%S')}"
                st.metric("Data Window", time_range_str)
            
            st.markdown("---")
            st.subheader("Real-time Market Trend")
            all_coins = sorted(buffer_data['cryptocurrency'].unique().tolist())
            
            f_col1, f_col2 = st.columns([3, 1])
            with f_col1:
                selected_coins = st.multiselect(
                    "Select Assets to Display",
                    options=all_coins,
                    default=all_coins,
                    key="rt_coin_filter"
                )
            with f_col2:
                use_log = st.checkbox("Logarithmic Y-Axis", value=False, key="rt_log_scale")
            
            filtered_price_data = buffer_data[buffer_data['cryptocurrency'].isin(selected_coins)]
            fig = px.line(
                filtered_price_data,
                x='timestamp',
                y='price_usd',
                color='cryptocurrency',
                markers=True,
                title="Synchronous Price Cluster ($1.00 - $8.00 Tier)",
                labels={'price_usd': 'Price (USD)', 'timestamp': 'System Time', 'cryptocurrency': 'Asset'}
            )
            if use_log:
                fig.update_yaxes(type="log")
            fig.update_layout(xaxis_title="Time", yaxis_title="Price (USD)", hovermode='x unified', margin=dict(t=50, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("View Raw Buffered Payloads (JSON Equivalent)"):
                display_df = buffer_data.sort_values('timestamp', ascending=False)
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=250)
        else:
            st.info("Awaiting live data from Kafka... Please ensure producer.py is streaming.")

def display_historical_view(config):
    st.markdown("### Historical Cloud Analytics (MongoDB Atlas)")
    
    with st.expander("Query Parameters", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            time_range = st.selectbox("Lookback Window", ["1h", "24h", "7d", "30d"], index=0)
        with col2:
            metric_type = st.selectbox("Tracked Metric", ["price_usd", "volume_24h_usd"])
        with col3:
            coin_filter = st.selectbox("Asset Filter", ["all", "uniswap", "toncoin", "polkadot", "near", 
                    "render-token", "cosmos", "filecoin", "sui", 
                    "optimism", "lido-dao"])
        with col4:
            aggregation = st.selectbox("Time Aggregation", ["raw", "hourly", "daily", "weekly"], index=0)
    
    historical_data = query_historical_data(
        time_range=time_range,
        metric_type=metric_type,
        coin_filter=coin_filter,
        aggregation=aggregation
    )
    
    if historical_data is not None and not historical_data.empty:
        s_col1, s_col2, s_col3 = st.columns(3)
        y_col = "volume_24h_usd" if metric_type == "volume_24h_usd" else "price_usd"
        y_label = "Volume (24h USD)" if metric_type == "volume_24h_usd" else "Price (USD)"
        
        with s_col1:
            st.metric("Total Records Queried", len(historical_data))
        with s_col2:
            st.metric(f"Average {y_label}", f"${historical_data[y_col].mean():,.2f}")
        with s_col3:
            val_std = historical_data[y_col].std()
            st.metric("Volatility (Std Dev)", f"${0.0 if pd.isna(val_std) else val_std:,.2f}")

        st.markdown("---")
        
        st.subheader(f"Historical Trend: {y_label}")
        use_hist_log = st.checkbox("Enable Logarithmic Scale", value=False, key="hist_log_scale")
        
        fig = px.line(
            historical_data.sort_values('timestamp'),
            x='timestamp',
            y=y_col,
            color='cryptocurrency',
            markers=True,
            labels={y_col: y_label, 'timestamp': 'Time', 'cryptocurrency': 'Asset'}
        )
        if use_hist_log:
            fig.update_yaxes(type="log")
        fig.update_layout(hovermode='x unified', margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("**Client-Side Export Active:** To export this exact view, hover over the top right of the chart and click the **Camera Icon** (Download plot as a png).")
        
        st.markdown("---")
        st.subheader("Export & Tabular View")
        
        with st.expander("Preview Historical Dataset", expanded=False):
            st.dataframe(historical_data, use_container_width=True, hide_index=True, height=250)
            
        export_col1, export_col2 = st.columns(2)
        
        with export_col1:
            st.markdown("**Raw Data**")
            csv_data = historical_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv_data,
                file_name="historical_crypto_data.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary"
            )
            
        with export_col2:
            st.markdown("**Data Report**")
            if st.button("Prepare PDF Report", use_container_width=True):
                with st.spinner("Generating Report..."):
                    try:
                        pdf_bytes = generate_text_pdf(historical_data, y_col, y_label)
                        st.session_state['pdf_export'] = pdf_bytes
                        st.success("PDF Generated! Click below.")
                    except Exception as e:
                        st.error(f"Error generating PDF: {e}")
                        
            if 'pdf_export' in st.session_state:
                st.download_button("Download PDF", data=st.session_state['pdf_export'], file_name="crypto_data_report.pdf", mime="application/pdf", use_container_width=True, type="primary")
            
    else:
        st.warning("No historical records matched the selected filters. Ensure historical_consumer.py is actively writing to MongoDB Atlas.")

def main():
    st.title("Big Data Crypto Dashboard")
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {'last_refresh': datetime.now(), 'auto_refresh': True}
    
    config = setup_sidebar()
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Refresh Engine")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox("Enable Auto Refresh", value=st.session_state.refresh_state['auto_refresh'])
    
    refresh_interval = 15
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    if st.sidebar.button("Fetch Latest Now", use_container_width=True):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    st.sidebar.info(f"Last Synced: {st.session_state.refresh_state['last_refresh'].strftime('%H:%M:%S')}")
    
    tab1, tab2 = st.tabs(["Real-time Streaming", "Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()
