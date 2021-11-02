import multiprocessing as mp
import time
import signal
import queue
from session_utils import sessioIdGen, sessionIdGen
from collections import defaultdict

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

def createDecisionMaker(exchangeQueue, userDataQueue, orderQueue, orderResponseQueue, wait_time=0.01):
    ...
    def decisionMaker(login):
        def readFromQueue(q, timeout=wait_time):
            try:
                block = timeout > 0 
                return q.get(block=block, timeout=timeout)
            except queue.Empty:
                return None

        orderIds = defaultdict(sessionIdGen)
        orderState = {}

        while True:
            exchOrder = readFromQueue(exchangeQueue, timeout=0)
            userData  = readFromQueue(userDataQueue, timeout=0)
            if ...:
                orderId = next(orderIds[login])
                tsOrder = ExecuteOrder(login=login, orderId=orderId)
                orderQueue.put(tsOrder)
            someOrderResponse = readFromQueue(orderResponseQueue, timeout=0)
            if someOrderResponse.good():
                pass
            else:
                cancelOrder = CancelOrder(login=login, orderId=someOrderResponse.orderId)
                orderQueue.put(cancelOrder)
            time.sleep(0.01)

        ...
    return decisionMaker

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
    
        

    
