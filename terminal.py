import json
import pika

# Set to store previously received coefficients
received_coefficients = set()

def main():
    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange for receiving processed results
    channel.exchange_declare(exchange='processed_results', exchange_type='fanout')

    # Declare a unique queue for this terminal
    result_queue = channel.queue_declare(queue='', exclusive=True).method.queue

    # Bind the queue to the exchange
    channel.queue_bind(exchange='processed_results', queue=result_queue)

    def callback(ch, method, properties, body):
        # Decode the data from JSON format
        data = json.loads(body)

        # Process the data based on its type
        if data['type'] == 'coefficient':
            meteo_coefficient = data['data']['meteo']
            pollution_coefficient = data['data']['pollution']

            # Check if coefficients have been received before
            if meteo_coefficient not in received_coefficients:
                received_coefficients.add(meteo_coefficient)
                print("Coefficient from result air sensor:", meteo_coefficient)

            if pollution_coefficient not in received_coefficients:
                received_coefficients.add(pollution_coefficient)
                print("Coefficient from pollution sensor:", pollution_coefficient)

    # Set up the callback function to consume messages
    channel.basic_consume(queue=result_queue, on_message_callback=callback, auto_ack=True)

    # Start consuming messages from the queue
    channel.start_consuming()

if __name__ == "__main__":
    main()



