from kafka import KafkaProducer
from time import sleep
import argparse
from datetime import datetime
from collections import defaultdict
import queue
import random

# Input file
DEFAULT_FILENAME = "test_x.csv"

# Rate is the required events per second. It is used
# to determine the needed delay between posts kafka
DEFAULT_RATE = 1000.0

# -1 specifies all
DEFAULT_COUNT = -1


def loader(filename):
    engines = defaultdict(queue.Queue)

    total_samples = 0
    with open(filename, "r") as file:
        for line in file:
            # Encode as bytes and strip trailing whitespaces
            engine_data = line.split(',')
            engine_id = engine_data[0]
            engines[engine_id].put(line)
            total_samples += 1

    return engines, total_samples


def get_engine_cycle(engines):
    engine_id = random.choice(list(engines))

    q = engines[engine_id]
    c = q.get()
    if q.qsize() == 0:
        del engines[engine_id]

    return c


def main(broker, topic, rate, source, count):
    """Main function that acts as a Kafka producer. It reads from a CSV file
    containing engine cycle statistics and posts each to a Kafka topic. It maintains
    cycle order.

    Args:
        broker (str): Broke in host:port format.
        topic (str): Topic to listen on.
        rate (float): The number of messages to be send per second. This is a best effort.
        source (str): The filename to read the stock transactions from.
    """

    producer = KafkaProducer(bootstrap_servers=broker)

    engines, num_samples = loader(source)

    if count == -1:
        count = num_samples

    engine_cycle = get_engine_cycle(engines)
    while count > 0 or engine_cycle is not None:
        # Post to the topic
        print("{}, Sending: {}".format(datetime.now(), engine_cycle))

        # Encode as bytes and strip trailing whitespaces
        msg = engine_cycle.rstrip().encode('utf-8')
        producer.send(topic, msg)
        count -= 1

        # Calculate the delay based on the rate.
        sleep(1.0 / rate)

        engine_cycle = get_engine_cycle(engines)

    # Push put any lingering messages then close
    producer.flush()
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("broker", help="host:port of the kafka broker.")
    parser.add_argument("topic", help="Kafka topic to post orders to.")
    parser.add_argument("-r", "--rate", type=float, default=DEFAULT_RATE, help="Rate to post messages")
    parser.add_argument("-s", "--source", default=DEFAULT_FILENAME, help="CSV containing messages")
    parser.add_argument("-c", "--count", default=DEFAULT_COUNT, help="Number of samples to send")

    args = parser.parse_args()
    main(args.broker, args.topic, args.rate, args.source, args.count)
