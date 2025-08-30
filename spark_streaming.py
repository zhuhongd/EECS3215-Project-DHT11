# spark_streaming.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, when

# --- Config via env ---
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "your_email@gmail.com")
EMAIL_PASSWORD= os.getenv("EMAIL_PASSWORD", "")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "recipient_email@example.com")
TEMP_THRESHOLD = float(os.getenv("TEMP_THRESHOLD", "40"))
HUMI_THRESHOLD = float(os.getenv("HUMI_THRESHOLD", "80"))
SOURCE_DIR    = os.getenv("SOURCE_DIR", "sensor_data")
CHECKPOINT    = os.getenv("CHECKPOINT", "_checkpoints/iot_alerts")

def send_email_alert(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = RECIPIENT_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Alert email sent.")
    except Exception as e:
        print(f"Failed to send email: {e}")

spark = SparkSession.builder.appName("Real-Time IoT Alerts").getOrCreate()

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("temperature", StringType()) \
    .add("humidity", StringType())

data_stream = (
    spark.readStream
         .schema(schema)
         .option("header", "true")
         .csv(SOURCE_DIR)
)

# Cast to numeric safely
typed = (data_stream
         .withColumn("temperature", col("temperature").cast(DoubleType()))
         .withColumn("humidity", col("humidity").cast(DoubleType())))

processed = typed.withColumn(
    "alert",
    when((col("temperature") > TEMP_THRESHOLD) | (col("humidity") > HUMI_THRESHOLD), "ALERT")
    .otherwise("NORMAL")
)

def process_alerts(batch_df, batch_id):
    alerts = batch_df.filter(col("alert") == "ALERT") \
                     .select("timestamp", "temperature", "humidity") \
                     .collect()
    for row in alerts:
        subject = "IoT Alert: Threshold Breach"
        body = f"Timestamp: {row['timestamp']}\nTemp: {row['temperature']} °C\nHumidity: {row['humidity']} %"
        send_email_alert(subject, body)

query = (processed.writeStream
         .foreachBatch(process_alerts)
         .outputMode("update")
         .option("checkpointLocation", CHECKPOINT)
         .start())

query.awaitTermination()
