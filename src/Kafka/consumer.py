from confluent_kafka import DeserializingConsumer
import json
import time

def run_consumer():
    d = DeserializingConsumer({'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9093',
                  'security.protocol': 'sasl_ssl', 'sasl.mechanism': 'SCRAM-SHA-512',
                  'sasl.username': 'demo-user', 'sasl.password': '291089',
                  'ssl.ca.location': '/home/bhishm/kafka/ssl/ca-cert',
                  'group.id': 'demo-consumer'
                  })
    running = True
    try:
        d.subscribe(['demo-topic'])

        while running:
            msg = d.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # x = str(msg.value().decode('utf-8')).replace("'", '"')
                return msg.value().decode('utf-8')
                # y = json.loads(x)
                # print(y['name'])
    finally:
    # Close down consumer to commit final offsets.
        d.close()

if __name__ == '__main__':
    run_consumer()