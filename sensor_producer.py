from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    my_name = "oleksbod"
    topic_name = f"{my_name}_building_sensors"

    sensor_id = str(uuid.uuid4())
    print(f"Sensor started → ID: {sensor_id}")

    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }

        producer.send(topic_name, value=data)
        producer.flush()

        print(f"Sent → {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("\nSensor stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'producer' in locals():
        producer.close()
