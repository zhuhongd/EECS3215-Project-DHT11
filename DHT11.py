# DHT11.py
import Adafruit_DHT
import time
import csv
import os
from datetime import datetime

DHT_SENSOR = Adafruit_DHT.DHT11
DHT_PIN = 7

OUT_DIR = "sensor_data"                 # streaming source dir
ROTATE_EVERY_SEC = 60                   # new file every N seconds
HEADER = ["timestamp", "temperature", "humidity"]

os.makedirs(OUT_DIR, exist_ok=True)

def _new_path(now=None):
    now = now or datetime.now()
    # one file per minute: e.g. reading_2025-08-30_09-12.csv
    fname = f"reading_{now.strftime('%Y-%m-%d_%H-%M')}.csv"
    return os.path.join(OUT_DIR, fname)

def log_sensor_data():
    cur_path = None
    cur_min = None

    while True:
        humidity, temperature = Adafruit_DHT.read(DHT_SENSOR, DHT_PIN)

        if humidity is not None and temperature is not None:
            now = datetime.now()
            # rotate file when minute changes
            if cur_min != (now.year, now.month, now.day, now.hour, now.minute):
                cur_min = (now.year, now.month, now.day, now.hour, now.minute)
                cur_path = _new_path(now)
                if not os.path.exists(cur_path):
                    with open(cur_path, "w", newline="") as f:
                        csv.writer(f).writerow(HEADER)

            row = [now.strftime("%Y-%m-%d %H:%M:%S"), round(temperature, 2), round(humidity, 2)]
            with open(cur_path, "a", newline="") as f:
                csv.writer(f).writerow(row)

            print(f"Logged data -> {cur_path}: {row}")
        else:
            print("Sensor failure. Check wiring.")

        time.sleep(2)

if __name__ == "__main__":
    log_sensor_data()
