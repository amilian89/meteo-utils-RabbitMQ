import time
import json
import pika
from meteo_utils import MeteoDataDetector

def main():
    # Instantiate the MeteoDataDetector class
    detector = MeteoDataDetector()

    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Create a queue to send the messages to
    channel.queue_declare(queue='sensor_data')

    while True:
        # Generate air and pollution data
        print("Sending following MeteoData and PollutionData:")
        meteo_data = detector.analyze_air()  # Air sensors
        pollution_data = detector.analyze_pollution()  # Pollution sensors
        print(meteo_data)
        print(pollution_data)
        print("................")

        # Convert the data to JSON format and add the data type
        meteo_data_json = json.dumps({'type': 'meteo', 'data': meteo_data})
        pollution_data_json = json.dumps({'type': 'pollution', 'data': pollution_data})

        # Send the data to the RabbitMQ queue
        channel.basic_publish(exchange='', routing_key='sensor_data', body=meteo_data_json)
        channel.basic_publish(exchange='', routing_key='sensor_data', body=pollution_data_json)

        # Wait for a specified amount of time before the next iteration
        time.sleep(5)

    # Close the connection
    connection.close()

if __name__ == "__main__":
    main()
