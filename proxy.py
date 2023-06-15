import time
import json
import pika
import redis

def main():
    # Establish a connection to Redis
    redis_client = redis.Redis()

    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange for publishing processed results
    channel.exchange_declare(exchange='processed_results', exchange_type='fanout')

    # Set to store the coefficients that have been published
    published_coefficients = set()

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

            # Check if coefficients have been published before
            if meteo_coefficient not in published_coefficients:
                # Create a message dictionary with the meteo and pollution coefficients
                message = {
                    'type': 'coefficient',
                    'data': {
                        'meteo': meteo_coefficient,
                        'pollution': pollution_coefficient
                    }
                }

                # Convert the message to JSON format
                message_json = json.dumps(message)

                # Publish the coefficients to the exchange
                channel.basic_publish(exchange='processed_results', routing_key='', body=message_json)

                # Add the coefficients to the published set
                published_coefficients.add(meteo_coefficient)
                published_coefficients.add(pollution_coefficient)

        # Delay before checking for new results
        time.sleep(3)

    # Close the RabbitMQ connection
    connection.close()

if __name__ == "__main__":
    main()



