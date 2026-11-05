import os
import sys
import pyspark
from pymongo import MongoClient

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ.pop("HADOOP_CONF_DIR", None)
os.environ.pop("YARN_CONF_DIR", None)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

MONGO_URI = "mongodb+srv://ivanrextamondong_db_user:ivanrextamondong_db_password@groceryinventorysystem.lnobtgv.mongodb.net/?retryWrites=true&w=majority"

spark = SparkSession.builder \
    .appName("CryptoHistoricalPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting PySpark Historical Pipeline...")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("cryptocurrency", StringType()) \
    .add("price_usd", DoubleType()) \
    .add("volume_24h_usd", DoubleType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def write_to_mongo(batch_df, batch_id):
    records = [
        row.asDict() for row in batch_df.collect()
        if row["timestamp"] is not None and row["cryptocurrency"] is not None and row["price_usd"] is not None
    ]
    if records:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=20000, maxPoolSize=50)
            db = client["crypto_db"]
            collection = db["historical_data"]
            
            collection.insert_many(records, ordered=False)
            client.close()
            print(f"Batch {batch_id}: Successfully inserted {len(records)} valid crypto records into MongoDB Atlas.")
        except Exception as e:
            print(f"Batch {batch_id} write failed: {e}")

query = parsed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "file:///tmp/spark_checkpoints/crypto_clean_v1") \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .start()

query.awaitTermination()
