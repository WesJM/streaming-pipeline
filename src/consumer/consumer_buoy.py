from confluent_kafka import Consumer
import pandas as pd
import json, os, io
from datetime import datetime
import boto3
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv()

# Kafka topic identifier
TOPIC = os.getenv('KAFKA_TOPIC', 'buoy_station_51004')

# Batch number of messages to process
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10))

# S3 config (from env or hardcoded for testing)
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX', 'buoy/curated/parquet')

# Kafka bootstrap.servers
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
GROUP_ID = os.getenv('CONSUMER_GROUP', 'buoy-curated-consumer')


SCHEMA = {
    "timestamp": 'string',
    'wind_dir_deg': 'float64',
    'wind_speed_mps': 'float64',
    'wind_gust_mps': 'float64',
    'wave_height_m': 'float64',
    'wave_period_s': 'float64',
    'wave_dir_deg': 'float64',
    'pressure_hpa': 'float64',
    'air_temp_c': 'float64',
    'water_temp_c': 'float64',
    'dewpoint_temp_c': 'float64',
    'visibility_nmi': 'float64',
    'tide_m': 'float64',
    'wind_category': 'string',
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
)

consumer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_conf)


def to_float(val):
    try:
        # "MM" is "Missing Measurment" for NOAA data. Treat “MM” and blanks as nulls
        if val in [None, '', 'MM']: # 
            return None
        return float(val)
    except Exception:
        return None


# Beaufort scale helper
def classify_wind(speed):
    try:
        if speed is None:
            return 'Unknown'
        speed = float(speed)
    except Exception:
        return 'Unknown'

    speed = float(speed)
    if speed < 0.5: return 'Calm'
    elif speed < 1.5: return 'Light air'
    elif speed < 3.3: return 'Light breeze'
    elif speed < 5.5: return 'Gentle breeze'
    elif speed < 7.9: return 'Moderate breeze'
    elif speed < 10.7: return 'Fresh breeze'
    elif speed < 13.8: return 'Strong breeze'
    elif speed < 17.1: return 'Near gale'
    elif speed < 20.7: return 'Gale'
    elif speed < 24.4: return 'Strong gale'
    elif speed < 28.4: return 'Storm'
    elif speed < 32.6: return 'Violent storm'
    else: return 'Hurricane'


# Transform a single record
def transform_record(raw):
    try:
        ts = datetime(
            int(raw['YY']), int(raw['MM']), int(raw['DD']),
            int(raw['hh']), int(raw['mm'])
        ).strftime("%Y-%m-%d %H:%M:%S") # ISO8601 string
    except Exception:
        ts = None

    return {
        'timestamp': ts,                                # Date/time parts
        'wind_dir_deg': to_float(raw.get('WD')),        # Wind direction (degrees)
        'wind_speed_mps': to_float(raw.get('WSPD')),    # Wind speed (m/s)
        'wind_gust_mps': to_float(raw.get('GST')),      # Wind gust (m/s)
        'wave_height_m': to_float(raw.get('WVHT')),     # Wave height (m)
        'wave_period_s': to_float(raw.get('DPD')),      # Dominant wave period (s)
        'wave_dir_deg': to_float(raw.get('MWD')),       # Mean wave direction (deg)
        'pressure_hpa': to_float(raw.get('PRES')),      # Sea-level pressure (hPa)
        'air_temp_c': to_float(raw.get('ATMP')),        # Air temp (°C)
        'water_temp_c': to_float(raw.get('WTMP')),      # Water temp (°C)
        'dewpoint_temp_c': to_float(raw.get('DEWP')),   # Dewpoint temp (°C)
        'visibility_nmi': to_float(raw.get('VIS')),     # Visibility (nmi)
        'tide_m': to_float(raw.get('TIDE')),            # Tide (m)
        'wind_category': classify_wind(to_float(raw.get('WSPD', 0))),   # Beaufort Wind Scale (mph)
    }


# Upload curated batch to S3
def upload_curated(records):

    if not records:
        logging.warning('No records to upload.')
        return
    if not S3_BUCKET:
        logging.error('S3_BUCKET not set. Skipping upload.')
        return
    
    df = pd.DataFrame(records)
    try:
        df = df.astype(SCHEMA)
    except Exception as e:
        logging.error(f"Schema enforcement failed: {e}")
        return

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f'{S3_PREFIX}/curated_batch_{ts}.parquet'

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.upload_fileobj(buffer, S3_BUCKET, key)
    logging.info(f'Uploaded curated {len(df)} records to s3://{S3_BUCKET}/{key}')


def main():
    consumer.subscribe([TOPIC])
    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            raw = json.loads(msg.value().decode('utf-8'))
            transformed = transform_record(raw)
            buffer.append(transformed)

            if len(buffer) >= BATCH_SIZE:
                upload_curated(buffer)
                buffer = []

    except KeyboardInterrupt:
        print('Stopping consumer...')
    finally:
        if buffer:
            upload_curated(buffer)
        consumer.close()

if __name__ == '__main__':
    main()