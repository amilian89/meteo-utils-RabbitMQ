import json
import pika
from meteo_utils import MeteoDataProcessor

def main():
    # Instantiate the MeteoDataProcessor class
    processor = MeteoDataProcessor()

    # Set up the RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the queue from which the messages will be received
    channel.queue_declare(queue='sensor_data')

    # Declare an exchange to which the processed results will be published
    channel.exchange_declare(exchange='processed_results', exchange_type='fanout')

    class MeteoData:
        def __init__(self, temperature, humidity):
            self.temperature = temperature
            self.humidity = humidity

    class PollutionData:
        def __init__(self, co2):
            self.co2 = co2

    def callback(ch, method, properties, body):
        # Decode the data from JSON format
        data = json.loads(body)

        # Process the data based on its type
        if data['type'] == 'meteo':
            meteo_data = MeteoData(data['data']['temperature'], data['data']['humidity'])
            result_air_sensor = processor.process_meteo_data(meteo_data)
            print("Air sensor coefficient:", result_air_sensor)
            result = {'type': 'meteo', 'data': result_air_sensor}
        elif data['type'] == 'pollution':
            pollution_data = PollutionData(data['data']['co2'])
            result_pollution_sensor = processor.process_pollution_data(pollution_data)
            print("Pollution sensor coefficient:", result_pollution_sensor)
            result = {'type': 'pollution', 'data': result_pollution_sensor}

        # Convert the result to JSON format
        message = json.dumps(result)

        # Publish the result to the exchange
        channel.basic_publish(exchange='processed_results', routing_key='', body=message)

    # Start consuming messages from the queue
    channel.basic_consume(queue='sensor_data', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()
