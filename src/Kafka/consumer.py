from confluent_kafka import DeserializingConsumer
import mysql.connector
from mysql.connector import Error
import json

def create_server_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database = db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def create_retailer(retail_id, retail_name):
    q1 = f"""
    INSERT INTO retailers (retail_id, retail_name)
    VALUES ({retail_id}, '{retail_name}');
    """
    execute_query(connection, q1)

def add_transaction(client_id, retail_id, item_id, cost_price, BorS, consignment_no, transaction_date):
    add_query = f"""
    INSERT INTO {BorS} (client_id, retail_id, item_id, cost_price, consignment_no, transaction_date)
    VALUES ('{client_id}', '{retail_id}', '{item_id}', '{cost_price}', '{consignment_no}', '{transaction_date}');
    """
    execute_query(connection, add_query)

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
                x = str(msg.value().decode('utf-8')).replace("'", '"')
                y = json.loads(x)
                print(msg.value().decode('utf-8'))
                add_transaction(**y)
    finally:
    # Close down consumer to commit final offsets.
        d.close()



if __name__ == '__main__':
    connection = create_server_connection('localhost', 'root', "", 'store')
    run_consumer()