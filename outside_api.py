import gzip
import requests
import json
import asyncio
import websockets
import logging

import time
import numpy as np
import math
import sys

import queue

from binance.client import AsyncClient
from binance import BinanceSocketManager


class FTXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://ftx.com/ws/"
        self.logger = logger
        self.callbacks = []
        self.last_ping = 0

    async def connect(self):
        self.websocket = await websockets.connect(self.host, ping_interval=None)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic):
        data = {'op': 'subscribe', 'channel': topic, 'market': 'BTC-PERP'}
        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()
            #message = json.loads(message)
            for callback in self.callbacks:
                callback("ftx", message)
            # print("Debug")
            '''
            if (time.time() - self.last_ping > 10.0):
                ping = json_dumps({'op': 'ping'})
                print(ping)
                await self.websocket.send(ping)
                self.last_ping = time.time()
            '''


class BitMEXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket = await websockets.connect(self.host)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic):
        data = {'op': 'subscribe', 'channel': topic, 'market': 'BTC-PERP'}
        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()
            #message = json.loads(message)
            for callback in self.callbacks:
                callback("bitmex", message)


class ByBitAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://stream.bybit.com/realtime"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket = await websockets.connect(self.host)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self):
        data = {"op": "subscribe", "args": ["trade.BTCUSD"]}
        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()
            #message = json.loads(message)
            for callback in self.callbacks:
                callback("bybit", message)


class HuobiAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://api.hbdm.com/swap-ws"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket = await websockets.connect(self.host)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self):
        data = {"sub": "market.BTC-USD.trade.detail"}
        print("huobi subscribing", json.dumps(data))
        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            rsp = await websocket.recv()
            raw_data = gzip.decompress(rsp).decode()
            data = json.loads(raw_data)
            #print("huobi", data)
            if "op" in data and data.get("op") == "ping":
                pong_msg = {"op": "pong", "ts": data.get("ts")}
                await websocket.send(json.dumps(pong_msg))
                #print(f"send: {pong_msg}")
                continue
            if "ping" in data:
                pong_msg = {"pong": data.get("ping")}
                await websocket.send(json.dumps(pong_msg))
                #print(f"send: {pong_msg}")
                continue
            #message = json.loads(message)
            for callback in self.callbacks:
                callback("huobi", raw_data)


class BinanceAPI:
    def __init__(self, logger=logging.getLogger(), api_key=None, api_secret=None):

        self.client = AsyncClient(api_key, api_secret)
        self.bm = BinanceSocketManager(self.client)
        self.logger = logger
        self.callbacks = []
        self.event_loop = asyncio.get_event_loop()

        self.queue = asyncio.Queue()

    def subscribe_futures(self, symbol):
        
        self.event_loop.create_task(self.loop(symbol))

    def on_message(self, callback):
        self.callbacks.append(callback)

    def process_message(self, message):
        data = message['data']
        asyncio.run_coroutine_threadsafe(self.queue.put(data), self.event_loop)

    async def loop(self, symbol):
        async with self.bm.aggtrade_futures_socket(symbol) as ts:
            while True:
                data = await ts.recv() 
                if data != None:
                    for callback in self.callbacks:
                        callback('binance-s', data)

    # def stop(self):

    #     self.bm.stop()


def test_callback(source, raw_data):
    timestamp = time.time()
    logging.info("%f %-10s: %s" % (timestamp, source, raw_data))


async def main():
    bybit_api = ByBitAPI()
    ftx_api = FTXAPI()
    binance_api = BinanceAPI()
    bitmex_api = BitMEXAPI()
    huobi_api = HuobiAPI()

    logging.basicConfig(level=logging.INFO, filename="dump.log")

    await bybit_api.connect()
    await bybit_api.subscribe()
    bybit_api.on_message(test_callback)

    binance_api.subscribe_futures('BTCUSDT')
    binance_api.on_message(test_callback)
    binance_api.stop()

    await ftx_api.connect()
    # await ftx_api.subscribe('ticker')
    # await ftx_api.subscribe('orderbook')
    await ftx_api.subscribe('trades')
    ftx_api.on_message(test_callback)

    await bitmex_api.connect()
    bitmex_api.on_message(test_callback)

    await huobi_api.connect()
    await huobi_api.subscribe()
    huobi_api.on_message(test_callback)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()
