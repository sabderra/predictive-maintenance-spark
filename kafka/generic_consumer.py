"""
Test consumer

python kafka/generic_consumer.py localhost:9092 engine_cycles
"""

import argparse
from kafka import KafkaConsumer
from datetime import datetime


def main(broker, topic):
    """Main function that indefinately consumes messages from a Kafka topic.

    Args:
        broker (str): Broke in host:port format.
        topic (str): Topic to listen on.
    """

    consumer = KafkaConsumer(topic, bootstrap_servers=broker)
    for msg in consumer:
        print("Alert Received: {}: {}".format( datetime.now(), msg.value))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("broker", help="host:port of the kafka broker.")
    parser.add_argument("topic", help="Kafka topic to post orders to.")

    args = parser.parse_args()

    print("Listening on: Broker={}, Topic={}".format(args.broker, args.topic))

    main(args.broker, args.topic)
