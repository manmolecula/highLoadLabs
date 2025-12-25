import csv
import json
import time
import os
import logging
import sys
from datetime import datetime
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'source_events_v2')
DATA_DIR = os.getenv('DATA_DIR', '/app/mock_data')
DELAY_MS = 0 

logger.info(f"KAFKA_BROKER: {KAFKA_BROKER}")
logger.info(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
logger.info(f"DATA_DIR: {DATA_DIR}")
logger.info(f"DELAY_MS: {DELAY_MS}")

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def create_producer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'python-producer',
        'acks': 1,
        'retries': 3,
        'linger.ms': 100,
        'batch.size': 65536, # 64KB
        'compression.type': 'snappy'
    }
    try:
        return Producer(conf)
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        return None

def parse_date_flexible(date_str):
    if not date_str or str(date_str).strip() == '':
        return None
    date_str = str(date_str).strip()
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%fZ']
    for fmt in formats:
        try:
            dt_obj = datetime.strptime(date_str, fmt)
            return dt_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        except ValueError:
            continue
    return date_str

def try_parse_int(value):
    if value is None or str(value).strip() in ['', 'NULL']: return None
    try: return int(float(value))
    except: return None

def try_parse_float(value):
    if value is None or str(value).strip() in ['', 'NULL']: return None
    try: return float(value)
    except: return None

def process_row(row_raw):
    row = dict(row_raw)
    try:
        row['id'] = try_parse_int(row.get('id'))
        row['sale_customer_id'] = try_parse_int(row.get('sale_customer_id'))
        row['sale_product_id'] = try_parse_int(row.get('sale_product_id'))
        row['sale_seller_id'] = try_parse_int(row.get('sale_seller_id'))
        
        row['customer_age'] = try_parse_int(row.get('customer_age'))
        row['product_quantity'] = try_parse_int(row.get('product_quantity'))
        row['sale_quantity'] = try_parse_int(row.get('sale_quantity'))
        row['product_reviews'] = try_parse_int(row.get('product_reviews'))
        
        row['product_price'] = try_parse_float(row.get('product_price'))
        row['sale_total_price'] = try_parse_float(row.get('sale_total_price'))
        row['product_weight'] = try_parse_float(row.get('product_weight'))
        row['product_rating'] = try_parse_float(row.get('product_rating'))
        
        row['sale_date'] = parse_date_flexible(row.get('sale_date'))
        row['product_release_date'] = parse_date_flexible(row.get('product_release_date'))
        row['product_expiry_date'] = parse_date_flexible(row.get('product_expiry_date'))
        
        return row
    except Exception as e:
        logger.error(f"Error processing row: {e}")
        return None

def send_data(producer, topic, data_files_dir):
    try:
        all_files = os.listdir(data_files_dir)
        csv_files = sorted([f for f in all_files if f.lower().endswith('.csv')])
        logger.info(f"Found files in {data_files_dir}: {csv_files}")
    except Exception as e:
        logger.error(f"Error listing directory: {e}")
        return

    if not csv_files:
        logger.error("No CSV files found!")
        return

    total_messages = 0

    for file_name in csv_files:
        file_path = os.path.join(data_files_dir, file_name)
        logger.info(f"=== Processing {file_name} ===")
        
        count = 0
        null_products = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row_raw in reader:
                    row = process_row(row_raw)
                    if row is None: continue
                    
                    if row.get('sale_product_id') is None:
                        null_products += 1

                    producer.produce(
                        topic=topic,
                        value=json.dumps(row, default=str).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)
                    count += 1
                    total_messages += 1
            
            producer.flush(timeout=5)
            logger.info(f"✓ Finished {file_name}: {count} rows. (NULL products: {null_products})")
            
        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")

    logger.info(f"✓ ALL DONE. Total messages sent: {total_messages}")

def main():
    if not os.path.exists(DATA_DIR):
        logger.error(f"Data dir not found: {DATA_DIR}")
        exit(1)
        
    producer = create_producer()
    if not producer: exit(1)
    
    send_data(producer, KAFKA_TOPIC, DATA_DIR)
    producer.flush(timeout=10)
    producer.close()

if __name__ == "__main__":
    main()
