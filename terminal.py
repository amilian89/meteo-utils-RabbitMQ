import json
import pika

def main():
    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the queue to receive data from the proxy
    channel.queue_declare(queue='terminal_data')

    def callback(ch, method, properties, body):
        # Decode the data from JSON format
        data = json.loads(body)

        # Process the data based on its type
        if data['type'] == 'coefficient':
            meteo_coefficient = data['data']['meteo']
            pollution_coefficient = data['data']['pollution']

            # Print the coefficients
            print("Coefficient from result air sensor:", meteo_coefficient)
            print("Coefficient from pollution sensor:", pollution_coefficient)

    # Set up the callback function to consume messages
    channel.basic_consume(queue='terminal_data', on_message_callback=callback, auto_ack=True)

    # Start consuming messages from the queue
    channel.start_consuming()

if __name__ == "__main__":
    main()

