import os
import random
from confluent_kafka import SerializingProducer
import simplejson as json
import uuid
from datetime import datetime, timedelta
import time

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}


# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100


# Environment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')



start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

random.seed(42)

def get_next_time():
    """
    Returns the current time in the format YYYY-MM-DDTHH:MM:SS
    """
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time

def generate_gps_data(device_id , timestamp , vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp,location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'cameraID': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp , location):
    return{
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5,25),
        'weatherCondition': random.choice(['sunny','cloudy','rain']),
        'precipitation': random.uniform(0,25),
        'windspeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500)
    }

def simulate_vehicle_movement():
    """
    Simulates the movement of a vehicle towards Birmingham by 
    updating the latitude and longitude coordinates based on pre-calculated increments. 
    Adds randomness to simulate road travel.
    """
    global start_location

    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    """
    Generates vehicle data based on the provided 
    vehicle_id and updates the vehicle_id by simulating vehicle movement.
    """
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'porsche',
        'model': 'Model S',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not json serializable')

def delivery_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic,data):
    producer.produce(
        topic,
        key = str(data['id']),
        value = json.dumps(data, default= json_serializer).encode('utf-8'),
        on_delivery = delivery_report 
    )
    producer.flush()


def simulate_journey(producer,device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id , vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],vehicle_data['location'], camera_id='Cam-123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if(vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print("Vehicle has reached BIRMINGHAM.")
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data) 
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data) 
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data) 
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)  
        
        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS ,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer,'vehicle-car-1')
    
    except KeyboardInterrupt:
        print("Simulation ended by the user")

    except Exception as e:
        print(f"Unexpected Error Occurred: {e}")


