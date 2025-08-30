# run_both.py
import subprocess
import sys
import time
import os
import signal

def main():
    env = os.environ.copy()
    print("Starting sensor logger...")
    data_proc = subprocess.Popen([sys.executable, "DHT11.py"], env=env)

    # Give it a moment to create the first file
    time.sleep(5)

    print("Starting Spark streaming...")
    spark_proc = subprocess.Popen(["spark-submit", "spark_streaming.py"], env=env)

    try:
        # Wait for Spark to exit (Ctrl+C to stop)
        spark_proc.wait()
    except KeyboardInterrupt:
        print("Interrupted. Terminating processes...")
    finally:
        for p in (spark_proc, data_proc):
            if p and p.poll() is None:
                # Try graceful, then force
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        print("All processes stopped.")

if __name__ == "__main__":
    main()
