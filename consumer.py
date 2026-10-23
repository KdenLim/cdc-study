import json
from confluent_kafka import Consumer

consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                    'group.id': 'consumer-group',
                    'auto.offset.reset': 'earliest'})

consumer.subscribe(['shop.public.customers', 'shop.public.orders'])

print('consumer is running')

try:
    while True:
        msg = consumer.poll()
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        value = msg.value()
        event = json.loads(value)
        print(msg.topic() ,event['payload']['after'])

except KeyboardInterrupt:
    print("\nStopping Consumer")
finally:
    consumer.close()
