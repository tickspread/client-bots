import asyncio
import websockets
import json
import logging
from functools import partial
import multiprocessing as mp
from queue import Empty

from typing import Callable, AsyncGenerator

async def call_subscription(subscrMessage, subscrUrl):
    async with websockets.connect(subscrUrl, compression=None) as ws:
        await ws.send(json.dumps(subscrMessage)) 
        while True:
            yield (response := await ws.recv())

ftx_Eth_Perp_Subscriber = partial(                        
                        call_subscription,          
                        subscrMessage = {'op': 'subscribe', 'channel': 'trades', 'market': 'ETH-PERP'},
                        subscrUrl = "wss://ftx.com/ws/",
)

async def subscriptionConsumer(subscriber : AsyncGenerator, callback : Callable, headerLength=1):
    for i in range(headerLength):
        _ = await subscriber.__anext__()
    async for response in subscriber:
        callback(response)

queue = mp.Queue()

async def otherProcessMain():
    await subscriptionConsumer(ftx_Eth_Perp_Subscriber(), queue.put)

def ourProcessWorker(queue):
    import time
    for i in range(10000):
        try:
            resp = queue.get(block=False, timeout=0.1)
            print("Received: ", resp)
        except Empty:
            time.sleep(0.1)

def otherProcessWorker():
    asyncio.run(otherProcessMain())

def main():
    p1 = mp.Process(target=otherProcessWorker)
    p2 = mp.Process(target=ourProcessWorker, args=(queue,) )
    #  p1.daemon = True
    # p2.daemon = True
    p1.start()
    p2.start()

if __name__ == "__main__":
    main()