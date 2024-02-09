import pika
import json

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='mle_ingest')

# Define the message to send
message = {
    'key1': 'value1',
    'key2': 'value2',
    'key3': 'value3'
}

# Convert the message to JSON
message_json = json.dumps(message)

# Publish the message to the queue
channel.basic_publish(exchange='', routing_key='mle_ingest', body=message_json)

# Close the connection
connection.close()
