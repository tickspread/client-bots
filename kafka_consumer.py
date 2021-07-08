import sys
from confluent_kafka import Consumer, TopicPartition
import argparse

parser = argparse.ArgumentParser(description='Consume messages from Kafka.')
parser.add_argument('--topic', dest='topic', default="main",
                    help='set the topic to be consumed from (default: main)')

parser.add_argument('--group_id', dest='group_id', default="utils-consumer-gid",
                    help='set the group_id that will consume on (default: utils-consumer-gid)')

parser.add_argument('--host', dest='host', default="localhost",
                    help='set the host that will consume from (default: utils-consumer-gid)')

parser.add_argument('--start', dest='start', default="earliest",
                            help='set the offset that will consume from (default: earliest)')

parser.add_argument('--offset', dest='offset', default=-1,
                    help='Start reading from an offset in the topic (default: -1 the tail)')

args = parser.parse_args()

topic = args.topic
group_id = args.group_id
host = args.host
start = args.start
starting_offset = int(args.offset)

print(topic)
c = Consumer({
#    'bootstrap.servers': '10.10.2.46:9092',
    'bootstrap.servers': '%s:9092' % host,
    'group.id': group_id,
    'auto.offset.reset': start,
    'enable.auto.commit': 'false'
})

if starting_offset < 0:
    low, high = c.get_watermark_offsets(TopicPartition(topic, 0), cached=False)
    print("high", high)
    starting_offset = high + starting_offset
    if starting_offset < low:
        starting_offset = low

c.assign([TopicPartition(topic, 0, offset=starting_offset)])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Message [%d]:\n' % msg.offset() +'{}'.format(msg.value().decode('utf-8')))

c.close()

