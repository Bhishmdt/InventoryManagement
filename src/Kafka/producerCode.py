from confluent_kafka import Producer
from faker import Faker
from random import randint
import time

def delivery_report(err, msg):
    if err:
        if str(type(err).__name__) == 'KafkaError':
            print(f'Message delivery failed : {str(err)}')
            print(f"Message retry - {err.retriable()}")
        else:
            print(f"Message delivery failed : {str(err)}")
    else:
        print(f"Message is delivered to the partition {msg.partition()}; Offset - {msg.offset()}")
        print(f"{msg.value()}")

def run_producer():
    p = Producer({'bootstrap.servers':'localhost:9092,localhost:9093,localhost:9093',
                  'security.protocol':'sasl_ssl','sasl.mechanism':'SCRAM-SHA-512',
                  'sasl.username':'demo-user','sasl.password':'291089',
                  'ssl.ca.location':'/home/bhishm/kafka/ssl/ca-cert',
                  'acks':'-1','partitioner':'consistent_random','batch.num.messages':'2','linger.ms':'100',
                  'queue.buffering.max.messages':'1000','retries':'1'})
    #print(p)
    topic_info = p.list_topics()
    print(topic_info.topics)

    for i in range(0,1):
        msg_value = {"id" : randint(0, 100), "name" : Faker('en-US').name()}
        msg_header = {'source' : b'DEM'}
        while True:
            try:
                p.poll(timeout=0)
                p.produce(topic='demo-topic', value=str(msg_value), headers=msg_header, on_delivery=delivery_report)
                break
            except BufferError as buffer_error:
                print(f"{buffer_error} :: Waiting until Queue gets some free space")
                time.sleep(1)
    p.flush()

if __name__ == '__main__':
    run_producer()
