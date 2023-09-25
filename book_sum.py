import sys
from confluent_kafka import Consumer, TopicPartition
import argparse

from confluent_kafka import Consumer, KafkaError
import json

aggregated_visible_delta = {}
message_count = 0

print("Topic: ", 'results')

partition = 3
starting_offset = 0
topic = 'results'

c = Consumer({
    'bootstrap.servers': '127.0.0.1',
    'group.id': 'python_book_8',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
})

if starting_offset < 0:
    low, high = c.get_watermark_offsets(TopicPartition(topic, partition), cached=False)
    print("high", high)
    starting_offset = high + starting_offset
    if starting_offset < low:
        starting_offset = low

c.assign([TopicPartition(topic, partition, offset=starting_offset)])


def is_relevant_message(message):
    try:
        data = json.loads(message)
        if data['event'] in ['book_add', 'book_remove'] and data['market'] == 'XAU-TEST':
            return True
    except (json.JSONDecodeError, KeyError):
        pass
    return False

def process_message(message):
    global aggregated_visible_delta
    data = json.loads(message)
    side = data['side']
    price = data['price']

    if side not in aggregated_visible_delta:
        aggregated_visible_delta[side] = {}
    if price not in aggregated_visible_delta[side]:
        aggregated_visible_delta[side][price] = 0

    aggregated_visible_delta[side][price] += data['visible_delta']

no_message_time = 0

while True:
    msg = c.poll(timeout=1.0)  # poll every 1 second

    if msg is None:
        no_message_time += 1  # increment the no_message_time by 1 second
        if no_message_time >= 10:  # if no message for 10 seconds
            print("No messages received for 10 seconds. Closing c.")
            break
        continue
    else:
        no_message_time = 0  # reset the counter if a message is received

    if msg.error():
        print("c error: {}".format(msg.error()))
        continue

    message_count += 1
    if is_relevant_message(msg.value()):
        process_message(msg.value())

        # Print results every 100k messages
    if message_count % 10000 == 0:
        print(f"After processing {message_count} messages:")
        for side, prices in aggregated_visible_delta.items():
            for price, delta in prices.items():
                if delta != 0:
                    print(f"Side: {side}, Price: {price}, Total Visible Delta: {delta}")

            print("---------------------------")
    c.commit(asynchronous=True)




c.close()


# Print final results
print("Final results:")
for side, prices in aggregated_visible_delta.items():
    for price, delta in prices.items():
        if delta != 0:
            print(f"Side: {side}, Price: {price}, Total Visible Delta: {delta}")
