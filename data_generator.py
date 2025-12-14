from kafka import KafkaProducer
from random import randint
import datetime
import time
import json
def serializer(msg):
	return json.dumps(msg).encode('utf-8')
producer = KafkaProducer(
	bootstrap_servers=['localhost:9092'],
	value_serializer=serializer
)
def generate():
	thermometer_id = randint(1,20)
	measurement_time = datetime.datetime.utcnow().isoformat()
	temperature = randint(-50, 50)
	return {
	'thermometer_id': thermometer_id,
	'measurement_time': measurement_time,
	'temperature': temperature
	}
if __name__ == '__main__':
    while True:
        msg = generate()
        print(msg)
        producer.send('Temperatura', msg)
        time.sleep(randint(1,7))
