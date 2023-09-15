import asyncio
import argparse
from outside_api import PythXauAPI
from confluent_kafka import Consumer, Producer

parser = argparse.ArgumentParser(description='Change the market status')

parser.add_argument('--host', dest='host', default="localhost",
                    help='set the host that will consume from (default: localhost)')

parser.add_argument('--partition', dest='partition', default=3,
                    help='set the partition that will consume from (default: 0)')

parser.add_argument('--market', dest='market',
                    help='The market name',default='XAU-TEST')

parser.add_argument('--status', dest='status', help='open|close|protected|enabled')

parser.add_argument('--topic', dest='topic', help='The orders topic', default='orders')

args = parser.parse_args()

producer_config = {'bootstrap.servers': args.host + ":9092"}
producer = Producer(producer_config)


error_count = 0
market_open = False

def open_market():
    print("open market")
    producer.poll(0.0)
    producer.produce(topic=args.topic, partition=int(args.partition), value='{"event":"market_status", "market": "' + args.market + '", "market_status": "enabled"}')
    producer.produce(topic=args.topic, partition=int(args.partition), value='{"event":"market_status", "market": "' + args.market + '", "market_status": "open"}')
    producer.flush(timeout=15.0)

def close_market():
    print("close market")
    producer.poll(0.0)
    producer.produce(topic=args.topic, partition=int(args.partition), value='{"event":"market_status", "market": "' + args.market + '", "market_status": "close"}')
    producer.flush(timeout=15.0)

def open_xau_market(provider,message):
    global error_count
    global market_open
    if 'status' in message:
        if message['status'] == 'ok':
            if market_open == False:
                open_market()
                market_open = True
        else:
            error_count += 1
            if error_count > 10:
                close_market()
                market_open = True



async def main():
    api = PythXauAPI()
    api.on_message(open_xau_market);
    api.subscribe_index_price('XAU-TEST')

try:
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
except (Exception, KeyboardInterrupt) as e:
    print('ERROR', str(e))
    exit()
