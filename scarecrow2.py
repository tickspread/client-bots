import multiprocessing as mp
import time
import signal
import queue
from session_utils import sessionIdGen, TimeWindow
from collections import defaultdict
from datetime import datetime, timedelta
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

def createOrderController(exchangeQueue, userDataQueue, orderQueue, orderResponseQueue, decisionMaker, userLogin, wait_time=0.01, maxIncidentCount=3, incidentWindow=timedelta(minutes=5)):
    ...
    def orderController():
        def readFromQueue(q, timeout=wait_time):
            try:
                block = timeout > 0 
                return q.get(block=block, timeout=timeout)
            except queue.Empty:
                return None

        orderIds = defaultdict(sessionIdGen)
        orderState = {}
        incidentTally = TimeWindow(timewindow=incidentWindow, logFunction=logging.error)

        def initiateShutdown():
            ...

        while True:
            exchOrder = readFromQueue(exchangeQueue, timeout=0)
            decisionMaker.updateExchOrder(exchOrder)
            for orderId, tsOrder in decisionMaker.getTradingOrders():
                orderQueue.put(tsOrder)
                while (someUserDataUpdate := readFromQueue(userDataQueue, timeout=0)) is not None:
                    assert someUserDataUpdate.login == userLogin
                    decisionMaker.updateFailedOrders
                    if not someUserDataUpdate.good():
                        decisionMaker.updateFailedOrder(orderId)
                        for cancelOrder in decisionMaker.getCancelOrders():
                            orderQueue.put(cancelOrder)
                while (someOrderResponse := readFromQueue(orderResponseQueue)) is not None:
                    incidentWindow.append(someOrderResponse.message)
                    if len(incidentWindow) > maxIncidentCount:
                        initiateShutdown()
            
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
    
        

    
