from kafka import KafkaProducer
from json import dumps
import time
import csv
from configparser import ConfigParser

# Load configuration from config.ini
config = ConfigParser()
config.read('config.ini')

# Kafka configuration
TOPIC = config.get('kafka', 'topic')
BOOTSTRAP_SERVERS = config.get('kafka', 'bootstrap_servers')
FILEPATH = config.get('kafka', 'csv_filepath')

# Creating Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: dumps(x).encode('utf-8'),  # Serialize messages as JSON
    api_version=(0, 10, 1)
)

if __name__ == "__main__":
    print("Starting Kafka Producer...")
    print("Reading data from: {}".format(FILEPATH))
    print("Producing to topic: {}".format(TOPIC))

    # Open the CSV file
    with open(FILEPATH, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)  # Skip the header row

        for row in csv_reader:
            # Create a message based on your schema
            msg = {
                "Serial Number": row[0],
                "List Year": row[1],
                "Date Recorded": row[2],
                "Town": row[3],
                "Address": row[4],
                "Assessed Value": float(row[5]) if row[5] else 0.0,
                "Sale Amount": float(row[6]) if row[6] else 0.0,
                "Sales Ratio": float(row[7]) if row[7] else 0.0,
                "Property Type": row[8],
                "Residential Type": row[9]
            }

            # Print the message for debugging
            print("Producing: {}".format(msg))

            # Send the message to the Kafka topic
            producer.send(TOPIC, value=msg)

            # Simulate real-time ingestion with a small delay
            time.sleep(0.1)

    print("Finished producing messages to Kafka.")
