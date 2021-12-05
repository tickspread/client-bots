import multiprocessing as mp
import time
import signal
import queue
from dataclasses import dataclass
from session_utils import sessionIdGen, TimeWindow
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
import logging
import asyncio
import json
import websockets
from functools import partial

logging.basicConfig(format='%(levelname)s:%(asctime):%(message)s', level=logging.DEBUG)


async def exchange_call_subscription(subscrMessage, subscrUrl):
    async with websockets.connect(subscrUrl, compression=None) as ws:
        await ws.send(json.dumps(subscrMessage)) 
        while True:
            yield (response := await ws.recv())

async def subscriptionConsumer(subscriber : AsyncGenerator, callback : Callable, headerLength=1):
    for i in range(headerLength):
        _ = await subscriber.__anext__()
    async for response in subscriber:
        callback(response)


def createOrderExecutor(queue, initDetails):
    ...
    def orderExecutor(orderDetails):
        ...
    
    return orderExecutor

def createExchangeWatcher(queue, initDetails):
    ...
    def exchangeWatcher(*args):
        ...
    return exchangeWatcher

def createUserDataWatcher(queue, initDetais):
    ...
    def userDataWatcher(*args):
        ...
    return userDataWatcher

def readFromQueue(q, timeout):
    try:
        return q.get(block=(timeout > 0), timeout=timeout)
    except queue.Empty:
        return None

@dataclass
class Flag:
    value : bool

    def __bool__(self):
        return bool(self.value)

def createOrderController(exchangeQueue, userDataQueue, orderQueue, orderResponseQueue, decisionMaker, maxIncidentCount=3, incidentWindow=timedelta(minutes=5)):

    proceed = Flag(True)

    def shutdown(*args, **kwds):
        proceed.value = False

    signal.signal(signal.SIGTERM, shutdown)
    
    def orderController():
        incidentTally = TimeWindow(timewindow=incidentWindow, logFunction=logging.error, trigger=(maxIncidentCount, shutdown))

        while proceed:
            userDataUpdates = iter(partial(readFromQueue, userDataQueue, timeout=0), None)

            for userDataUpdate in userDataUpdates: 
                decisionMaker.updateTradingOrderStatus(userDataUpdate)

            exchangeOrders = iter(partial(readFromQueue, exchangeQueue, timeout=0), None)

            for o in exchangeOrders: 
                decisionMaker.updateExchangeOrder(o)

            for o in decisionMaker.getTradingOrders(): 
                orderQueue.put(o)

            orderResponses = iter(partial(readFromQueue, orderResponseQueue, timeout=0.001), None)
            badOrderResponses = filter(None, orderResponses)

            incidentTally.extend(b.message for b in badOrderResponses)

    return orderController

# 
# a sample exchange subscriber -- treat it as an example
#
ftx_Eth_Perp_Subscriber = partial(                        
                        exchange_call_subscription,          
                        subscrMessage = {'op': 'subscribe', 'channel': 'trades', 'market': 'ETH-PERP'},
                        subscrUrl = "wss://ftx.com/ws/",
)

ftx_BTC_Perp_Subscriber = partial(                        
                        exchange_call_subscription,          
                        subscrMessage = {'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'},
                        subscrUrl = "wss://ftx.com/ws/",
)


exchangeQueue = mp.Queue()

async def exchangeSubscriberMain(subscriber, queue):
    await subscriptionConsumer(subscriber(), queue.put)

def exchangeWatcherMain(queue):
    asyncio.gather( exchangeSubscriberMain(ftx_Eth_Perp_Subscriber, queue), 
                    exchangeSubscriberMain(ftx_BTC_Perp_Subscriber, queue),
                  )

def main():
    oe_proc = mp.Process(target=...)

    exchangeWatcherProc = mp.process(target=exchangeWatcherMain, args=(exchangeQueue,))

    decision_proc = mp.Process(target=...)

    allProcs = [oe_proc, exchangeWatcherProc, decision_proc]
    for p in allProcs:
        p.daemon = True
        p.start()

    while True:
        time.sleep(0.2)
        deadProcs = (p for p in allProcs if p.exitcode is not None)
        if next(deadProcs, None) is not None:
            for p in allProcs:
                p.terminate()
            break
    
        

    
