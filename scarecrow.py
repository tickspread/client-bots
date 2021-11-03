import multiprocessing as mp
import time
from datetime import datetime, timedelta
from tickspread_api import TickSpreadAPI
from collections import defaultdict
from session_utils import sessionIdGen


def orderMonitor(queue, makeReader, readerArgs, sleepInterval=0.01):
    orderReader = makeReader(*readerArgs)
    while (order := orderReader()):
        if order is not None:
            queue.put(order)
        else:
            time.sleep(sleepInterval)

def executionResultStatus(executionResults, cancelResults):
    ...

def makeExecutionProcessPool(n_processes=8):
    p = mp.Pool(n_processes)
    p.start()
    return p

def makeOrderExecutor(tsLogin, tsPassword, env):
    tickSpreadAPI = TickSpreadAPI(env=env)
    loginResult = tickSpreadAPI.login(tsLogin, tsPassword)
    assert loginResult, f"did not login successfully for {tsLogin=}, {tsPassword=}"
    idGen = sessionIdGen(tsLogin) 
    def f(amount, price, leverage, symbol, side, type):
        client_order_id = next(idGen)
        orderPlacementResult = tickSpreadAPI.create_order_sync(client_order_id, amount, price, leverage, symbol, side, type)
        assert orderPlacementResult == client_order_id, f"mismatch between {orderPlacementResult=} and {client_order_id=}"
        return orderPlacementResult
    return f

def makeOrderCanceller(*args):
    # initialize
    def f():
        ...
    return f

def executionResultSuccess(result):
    ...

def executeBatch(tsOrderQueue, executeOrderArgs, cancelOrderArgs):
    pool = makeExecutionProcessPool()
    executeOrder = makeOrderExecutor(*executeOrderArgs)
    cancelOrder = makeOrderCanceller(*cancelOrderArgs)
    while (orders := tsOrderQueue.get()):
        executionResults = pool.map(executeOrder, orders)
        ordersToCancel = [order for order, result in zip(orders, executionResults) if not executionResultSuccess(result)]
        # TODO: cancellations need done as quickly as possible (asynchroniously?)
        cancelResults = pool.map(cancelOrder, ordersToCancel) if ordersToCancel else None
        returnStatus = executionResultStatus(executionResults, cancelResults)
        tsOrderQueue.put(returnStatus)

def decisionMaker(exchangeOrderQueue, tsOrderQueue):
    while True:
        exchangeOrder = exchangeOrderQueue.get()
        if ...:
            tsOrderQueue.put( [...] )
            orderResponse = tsOrderQueue.get()
            # decide further action with respect to the success status of the order response  

def createBinanceOrderReader(*args):
    def readOrders():
        ...
    return readOrders

def createFtxOrderReader(*args):
    def readOrders():
        ...
    return readOrders

def runIt():
    exchangeOrderQueue = mp.Queue()
    tsOrderQueue       = mp.Queue()

    binanceMonitoringProcess = mp.Process(target=orderMonitor, args=(exchangeOrderQueue, createBinanceOrderReader, ("binance", "arguments")))
    ftxMonitoringProcess  = mp.Process(target=orderMonitor, args=(exchangeOrderQueue, createFtxOrderReader, ("gemini", "arguments")))

    orderExecutionProcess = mp.Process(target=executeBatch, args=(tsOrderQueue, ("exec", "args"), ("cancel", "args")))

    decisionMakingProcess   = mp.Process(target=decisionMaker, args=(exchangeOrderQueue, tsOrderQueue))

    binanceMonitoringProcess.start()
    ftxMonitoringProcess.start()
    decisionMakingProcess.start()
    decisionMakingProcess.start()

