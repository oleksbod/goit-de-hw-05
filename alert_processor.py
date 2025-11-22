from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from configs import kafka_config
import json
import sys

my_name = "oleksbod"
input_topic = f"{my_name}_building_sensors"
temp_topic = f"{my_name}_temperature_alerts"
hum_topic = f"{my_name}_humidity_alerts"

print(f"Connecting to Kafka broker at {kafka_config['bootstrap_servers']}...")

try:
    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f"{my_name}_processor_group",
        consumer_timeout_ms=10000
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=10000
    )
    print("Connected successfully!")
except NoBrokersAvailable as e:
    print(f"\n ERROR: Cannot connect to Kafka broker!")   
    sys.exit(1)
except Exception as e:
    print(f"\n ERROR: Failed to create consumer/producer: {e}")
    sys.exit(1)

print(f"Listening to topic: {input_topic}")
print(f"Alert topics: {temp_topic}, {hum_topic}")
print("Press Ctrl+C to stop\n")

try:
    for message in consumer:
        try:
            data = message.value
            temperature = data.get("temperature")
            humidity = data.get("humidity")
            sensor_id = data.get("sensor_id")
            timestamp = data.get("timestamp")

            if temperature is None or humidity is None:
                print(f"Warning: Missing data in message: {data}")
                continue

            if temperature > 40:
                alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "temperature": temperature,
                    "humidity": humidity,
                    "alert": "HIGH TEMPERATURE",
                    "message": f"Temperature {temperature}°C exceeds threshold of 40°C"
                }
                producer.send(temp_topic, value=alert)
                producer.flush()
                print(f"TEMP ALERT → {alert}")

            if humidity > 80 or humidity < 20:
                alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "temperature": temperature,
                    "humidity": humidity,
                    "alert": "ABNORMAL HUMIDITY",
                    "message": f"Humidity {humidity}% is outside acceptable range (20-80%)"
                }
                producer.send(hum_topic, value=alert)
                producer.flush()
                print(f"HUMIDITY ALERT → {alert}")
        except Exception as e:
            print(f"Error processing message: {e}")
            continue
except KeyboardInterrupt:
    print("\nAlert processor stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'consumer' in locals():
        consumer.close()
    if 'producer' in locals():
        producer.close()
