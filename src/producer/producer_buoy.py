from confluent_kafka import Producer
import requests
import time
import json
import logging
import os
from dotenv import load_dotenv


load_dotenv()
# Config from .env with sensible defaults
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
STATION_ID = os.getenv('STATION_ID', '51004')

TOPIC = os.getenv('KAFKA_TOPIC', 'buoy_station_51004')
URL = f'https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.txt'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


def fetch_latest():
    """Fetch most recent NOAA buoy record as dict, with basic validation."""

    try:
        resp = requests.get(URL, timeout=10)
        resp.raise_for_status()
        lines = resp.text.strip().splitlines()
        
        if len(lines) < 3:
            logging.warning('NOAA buoy data missing expected rows.')
            return None

        # First two lines are headers, third line is most recent observation
        headers = lines[0].strip('#').split()
        latest_values = lines[2].split()

        if len(latest_values) != len(headers):
            logging.warning('Header/row mismatch.')
            return None

        try:
            record = dict(zip(headers, latest_values))
            return record
        
        except Exception as e:
            logging.error(f'Failed to parse NOAA record: {e}')
            return None
        
    except requests.exceptions.RequestException as e:
        print(f'Data fetch requests failed: {e}')


def stream_buoy_data(poll_interval=60):
    seen_timestamps = set()

    while True:
        record = fetch_latest()
        if not record:
            time.sleep(poll_interval)
            continue

        timestamp = f'{record["YY"]}-{record["MM"]}-{record["DD"]} {record["hh"]}:{record["mm"]}'
        if timestamp not in seen_timestamps:
            event = json.dumps(record).encode('utf-8')
            producer.produce(TOPIC, event, callback=delivery_report)
            producer.flush()
            logging.info(f'Sent: {record}')
            seen_timestamps.add(timestamp)
        else:
            logging.info(f'No new data. Retrying in {poll_interval}-seconds.')

        time.sleep(poll_interval)

if __name__ == '__main__':
    stream_buoy_data(60)