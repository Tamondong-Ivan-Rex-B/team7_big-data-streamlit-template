# Big Data Streaming Dashboard

A real-time, end-to-end Big Data pipeline and interactive dashboard that ingests, processes, and visualizes cryptocurrency market data.

![Python](https://img.shields.io/badge/Python-3.10-blue.svg)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Message_Broker-black)
![PySpark](https://img.shields.io/badge/PySpark-Structured_Streaming-orange)
![MongoDB Atlas](https://img.shields.io/badge/MongoDB_Atlas-Cloud_NoSQL-green)
![Streamlit](https://img.shields.io/badge/Streamlit-UI-red)

## Overview
This project demonstrates the application of Big Data engineering principles to high-velocity financial streams. It utilizes the **CoinGecko API** to fetch live price and volume data for a curated set of tightly clustered Web3 tokens ($1.00 - $8.00 range), ensuring readable, dynamic visualizations without massive price outliers.

**Architecture Flow:**
`CoinGecko API` ➔ `Kafka` ➔ `PySpark` ➔ `MongoDB Atlas` ➔ `Streamlit`

## 📁 Repository Structure
```text
├── app.py                     # Streamlit frontend dashboard (Real-time & Historical UI)
├── producer.py                # Ingests CoinGecko API data into Kafka
├── historical_consumer.py     # PySpark streaming job (Kafka to MongoDB Atlas)
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

## Backend/Infrastructure (Ubuntu)
The backend pipeline requires Java (8 or 11) and Apache Kafka to be installed locally.

1. **Start the Kafka Broker** (KRaft mode):
   ```bash
   cd ~/kafka_2.13-4.1.1
   bin/kafka-server-start.sh config/server.properties
   ```
2. **Setup the Python Environment**:
   ```bash
   conda create -n bigdata python=3.10
   conda activate bigdata
   pip install -r requirements.txt
   ```
3. **Run the Pipeline** (in separate terminal tabs):
   ```bash
   python producer.py              # Starts API data ingestion
   python historical_consumer.py   # Starts PySpark processing to MongoDB
   streamlit run app.py            # Launches the local dashboard
   ```

## Frontend Only (Windows)
For team members focusing on the UI, only a lightweight Python environment is required. Kafka or PySpark is not needed.

1. **Setup Environment**:
   ```cmd
   python -m venv env
   env\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Run the Dashboard**:
   ```cmd
   streamlit run app.py
   ```
3. **Connect to Pipeline**: 
   * To view the **Real-time Tab**, ensure same network configuration as the Ubuntu host and update the "Kafka Broker" IP in the Streamlit sidebar (e.g., `192.168.1.X:9092`).
   * The **Historical Tab** requires no network setup, as it pulls processed data directly from the MongoDB Atlas cloud cluster.
