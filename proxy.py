import time
import json
import redis
import pika

def main():
    # Establish a connection to Redis
    redis_client = redis.Redis()

    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the queue for sending data to terminals
    channel.queue_declare(queue='terminal_data')

    while True:
        # Read the processed results from Redis
        meteo_result = redis_client.get('meteo')
        pollution_result = redis_client.get('pollution')

        # Check if the results exist in Redis
        if meteo_result and pollution_result:
            # Process the results as needed
            meteo_data = json.loads(meteo_result)
            pollution_data = json.loads(pollution_result)

            meteo_coefficient = meteo_data['data']
            pollution_coefficient = pollution_data['data']

            print("Coefficient from air sensor:", meteo_coefficient)
            print("Coefficient from pollution sensor:", pollution_coefficient)

            # Create a message dictionary with the coefficients
            message = {
                'type': 'coefficient',
                'data': {
                    'meteo': meteo_coefficient,
                    'pollution': pollution_coefficient
                }
            }

            # Convert the message to JSON format
            message_json = json.dumps(message)

            # Publish the message to the terminal queue
            channel.basic_publish(exchange='', routing_key='terminal_data', body=message_json)

        # Delay before checking for new results
        time.sleep(12)

    # Close the RabbitMQ connection
    connection.close()

if __name__ == "__main__":
    main()

