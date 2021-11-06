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

logging.basicConfig(format='%(levelname)s:%(asctime):%(message)s', level=logging.DEBUG)

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
            userUpdates = iter(partial(readFromQueue, userDataQueue, timeout=0), None)
            badUserUpdates = (u for u in userUpdates if not u.good())

            for bu in badUserUpdates: 
                decisionMaker.updateFailedOrder(bu.orderId)

            exchangeOrders = iter(partial(readFromQueue, exchangeQueue, timeout=0), None)

            for o in exchangeOrders: 
                decisionMaker.updateExchangeOrder(o)

            for o in decisionMaker.getTradingOrders(): 
                orderQueue.put(o)

            orderResponses = iter(partial(readFromQueue, orderResponseQueue, timeout=0.01), None)
            badOrderResponses = filter(None, orderResponses)

            incidentTally.extend(b.message for b in badOrderResponses)

    return orderController

def main():
    oe_proc = mp.Process(target=...)
    exw_ftx_proc = mp.Process(target=...)
    exw_gem_proc = mp.Process(target=...)
    decision_proc = mp.Process(target=...)

    allProcs = [oe_proc, exw_ftx_proc, exw_gem_proc, decision_proc]
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
    
        

    
