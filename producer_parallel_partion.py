#!/usr/bin/env python

import sys
import time
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import threading

def produce_messages(producer, topic, user_ids, products, count, partition):
    for _ in range(count):
        user_id = user_ids
        product = choice(products)
        producer.produce(topic, key=user_id, value=product, partition=partition, callback=delivery_callback)
        time.sleep(0.1)

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

if __name__ == '__main__':
    # Record the start time
    start_time = time.time()

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instances
    producer1 = Producer(config)
    producer2 = Producer(config)

    # Produce data by selecting random values from these lists.
    topic = "purchases_partition"

    data_1 = ['APPLE', 'BANANA', 'COCONUT']
    data_2 = ['dog', 'cat', 'giraffe', 'pig']

    count = 50  # Each producer will produce 50 messages

    # Create threads for each producer
    thread1 = threading.Thread(target=produce_messages, args=(producer1, topic, 'producer_1', data_1, count, 0))
    thread2 = threading.Thread(target=produce_messages, args=(producer2, topic, 'producer_2', data_2, count, 1))

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for the threads to finish
    thread1.join()
    thread2.join()

    # Block until the messages are sent.
    producer1.flush()
    producer2.flush()

    # Record the end time
    end_time = time.time()

    # Calculate and print the total execution time
    total_time = end_time - start_time
    print(f'Total execution time: {total_time:.2f} seconds')
