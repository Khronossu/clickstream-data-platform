import json
import time
import uuid
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

# Initialize Faker and Kafka Producer
fake = Faker()
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC = 'clickstream-events'

# Simulation constants
EVENT_TYPES = ['click', 'view', 'purchase', 'scroll']
PAGES = ['/home', '/products', '/checkout', '/blog', '/pricing', '/login']
DEVICES = ['mobile', 'desktop', 'tablet']

def delivery_report(err, msg):
    """Callback triggered by Kafka to check if message was delivered."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    # Uncomment the line below for heavy debugging, but it will flood your console
    # else:
    #    print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_event():
    """Generates a single clickstream event based on the defined schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1000, 9999)), # Simulating returning users
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "page": random.choice(PAGES),
        "device": random.choice(DEVICES),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def run_producer():
    print(f"Starting simulated event stream to topic: {TOPIC}...")
    try:
        while True:
            event = generate_event()
            # Produce message to Kafka
            producer.produce(
                topic=TOPIC,
                key=event["user_id"], # Keying by user_id ensures events for the same user go to the same partition
                value=json.dumps(event),
                callback=delivery_report
            )
            
            # Poll handles delivery reports
            producer.poll(0)
            
            # Sleep to simulate realistic traffic (e.g., 2-5 events per second)
            time.sleep(random.uniform(0.2, 0.5))
            
            print(f"Sent event: {event['event_type']} on {event['page']} by user {event['user_id']}")

    except KeyboardInterrupt:
        print("\nStopping event generation...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()

if __name__ == "__main__":
    run_producer()