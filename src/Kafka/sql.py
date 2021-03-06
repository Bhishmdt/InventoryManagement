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

if __name__ == '__main__':
    connection = create_server_connection('localhost', 'root', "", 'store')
    run_producer()
    value = run_consumer()
    x = str(value).replace("'", '"')
    y = json.loads(x)
    create_retailer(y['id'], str(y['name'].split()[0]))
