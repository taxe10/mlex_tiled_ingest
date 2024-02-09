import pika
import json


def callback(ch, method, properties, body):
    # Decode the JSON message
    message = json.loads(body)
    print(message)
    # Prcess the message
    # TODO: Add your logic here
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Connect to RabbitMQ
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='tomo_reconstruction')

# Set the prefetch count to limit the number of unacknowledged messages
channel.basic_qos(prefetch_count=1)

# Start consuming messages
channel.basic_consume(queue='tomo_reconstruction', on_message_callback=callback)

# Enter a loop to continuously consume messages
channel.start_consuming()
