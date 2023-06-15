import pika
import json

def main():
    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare an exchange from which the processed results will be received
    channel.exchange_declare(exchange='processed_results', exchange_type='fanout')

    # Declare a queue and bind it to the exchange
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='processed_results', queue=queue_name)

    def callback(ch, method, properties, body):
        # Decode the result from JSON format
        result = json.loads(body)

        # Update the UI based on the type of the result
        if result['type'] == 'meteo':
            print("Received air sensor coefficient:", result['data'])
        elif result['type'] == 'pollution':
            print("Received pollution sensor coefficient:", result['data'])

    # Start consuming messages from the queue
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()
