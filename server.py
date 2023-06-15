import json
import redis
import pika
from meteo_utils import MeteoDataProcessor

def main():
    # Instantiate the MeteoDataProcessor class
    processor = MeteoDataProcessor()

    # Establish a connection to Redis
    redis_client = redis.Redis()

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

        # Store the result in Redis
        redis_client.set(data['type'], json.dumps(result))

    # Start consuming messages from the queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data')
    channel.basic_consume(queue='sensor_data', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()

