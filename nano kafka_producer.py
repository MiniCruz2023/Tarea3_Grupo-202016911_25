from kafka import KafkaProducer
import json
import time
import random

# Crear el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Enviar datos simulados al tópico
try:
    while True:
        data = {
            'sensor_id': random.randint(1, 10),
            'timestamp': time.time(),
            'value': round(random.uniform(20.0, 30.0), 2)
        }
        print(f"Enviando: {data}")
		        producer.send('sensor_data', value=data)
        time.sleep(1)
except KeyboardInterrupt:
    print("Interrumpido por el usuario.")
finally:
    producer.close()

