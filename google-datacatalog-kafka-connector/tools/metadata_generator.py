import argparse
import logging
import random
import sys
import uuid
import math

from confluent_kafka.admin import AdminClient, NewTopic

_BATCH_SIZE = 50

_TOPIC_NAMES = [
    'school_visits', 'personal_info_updates', 'persons_visits',
    'employees_updates', 'companies_updates', 'goods_arrivals', 'home_events'
]


def create_random_metadata():
    admin_client = AdminClient({'bootstrap.servers': args.kafka_host})
    topic_attributes = create_new_topics_attributes()
    new_topics = [
        NewTopic(topic_name,
                 num_partitions=partitions,
                 replication_factor=replicas)
        for topic_name, partitions, replicas in topic_attributes
    ]
    num_batches = math.ceil(len(new_topics) / _BATCH_SIZE)
    for batch in range(num_batches):
        start_idx = batch * _BATCH_SIZE
        stop_idx = start_idx + _BATCH_SIZE
        topics_to_create = new_topics[start_idx:stop_idx]
        fs = admin_client.create_topics(topics_to_create, request_timeout=60.0)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))


def get_random_partition_number():
    return random.randint(1, args.max_partitions)


def get_random_replication_factor():
    return random.randint(1, args.max_replication_factor)


def get_random_topic_name():
    return random.choice(_TOPIC_NAMES)


def create_new_topics_attributes():
    topic_attributes = []
    for i in range(args.number_topics):
        # for i in range(3):
        topic_name = get_random_topic_name() + uuid.uuid4().hex[:8]
        num_partitions = get_random_partition_number()
        replication_factor = get_random_replication_factor()
        topic_attributes.append(
            (topic_name, num_partitions, replication_factor))

    return topic_attributes


def parse_args():
    parser = argparse.ArgumentParser(
        description='Command line generate random metadata into kafka')
    parser.add_argument(
        '--kafka-host',
        help='Your kafka bootstrap.servers configuration, required',
        required=True)
    parser.add_argument('--number-topics',
                        help='Number of topics to create',
                        type=int,
                        default=1000)
    parser.add_argument(
        '--max-replication-factor',
        help='Maximum replication factor for newly created topics, '
        'should be no less than number of brokers on your cluster',
        type=int,
        default=1)
    parser.add_argument('--max-partitions',
                        help='Maximum partitions number for generated topics',
                        type=int,
                        default=1)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # Enable logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    create_random_metadata()
