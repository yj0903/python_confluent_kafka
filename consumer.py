#!/usr/bin/env python

import sys
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING

if __name__ == '__main__':
    # Record the start time
    start_time = time.time()
    start_receive_time = time.time()

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)
    wait = False

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")

                if not wait:
                    # Calculate and print the waiting time
                    end_receive_time = time.time()
                    transfer_duration = end_receive_time - start_receive_time
                    print(f'Receive duration: {transfer_duration-1:.2f} seconds')
                    wait = True
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                start_waiting_time = time.time()  # Record the start time of waiting
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
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
