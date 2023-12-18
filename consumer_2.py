#!/usr/bin/env python

import sys
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError

if __name__ == '__main__':
    # Record the start time
    start_time = time.time()
    start_receive_time = time.time()

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Set unique group.id for each consumer group
    config['group.id'] = f"{config['group.id']}_{time.time()}"

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to the topic
    topic = "purchases"
    consumer.subscribe([topic])
    wait = False

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No new messages within the timeout
                print("Waiting...")

                if not wait:
                    # Calculate and print the waiting time
                    end_receive_time = time.time()
                    transfer_duration = end_receive_time - start_receive_time
                    print(f'Receive duration: {transfer_duration - 1:.2f} seconds')
                    wait = True
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event (not an error)
                    continue
                else:
                    print("ERROR: {}".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("[Consumer 2] Consumed event from topic {topic}, partition {partition}, offset {offset}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
                    key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                time.sleep(0.1)
                if wait:
                    wait = False
                    start_receive_time = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

        # Record the end time
        end_time = time.time()

        # Calculate and print the total execution time
        total_time = end_time - start_time
        print(f'Total execution time: {total_time:.2f} seconds')