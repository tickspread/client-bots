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
from binance import BinanceSocketManager, ThreadedWebsocketManager


class FTXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://ftx.com/ws/"
        self.logger = logger
        self.callbacks = []
        self.last_ping = 0

    async def connect(self):
        self.websocket = await websockets.connect(self.host, ping_interval=None)

    async def subscribe(self, topic):
        data = {'op': 'subscribe', 'channel': topic, 'market': 'ETH-PERP'}
        await self.websocket.send(json.dumps(data))
        asyncio.get_event_loop().create_task(self.loop(self.websocket, topic))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket, topic):
        while True:
            try:
                message = await websocket.recv()
                #message = json.loads(message)
                print("Debug")
                for callback in self.callbacks:
                    callback("ftx", message)
                print("Debug")
                '''
                if (time.time() - self.last_ping > 10.0):
                    ping = json_dumps({'op': 'ping'})
                    print(ping)
                    await self.websocket.send(ping)
                    self.last_ping = time.time()
                '''
            except Exception as e:
                print("FTX reconnect", e)
                await self.websocket.close()
                time.sleep(2)
                await self.connect()
                await self.subscribe(topic)
                break
            #message = await asyncio.wait_for(websocket.recv(), 5)


class BitMEXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket = await websockets.connect(self.host)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic):
        data = {'op': 'subscribe', 'channel': topic, 'market': 'ETH-PERP'}
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
        data = {"op": "subscribe", "args": ["trade.ETHUSD"]}
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
        data = {"sub": "market.ETH-USD.trade.detail"}
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
        print("wait bin")
        self.event_loop.create_task(self.loop(symbol))

    def on_message(self, callback):
        self.callbacks.append(callback)

    def process_message(self, message):
        # data = message['data']
        data = message
        asyncio.run_coroutine_threadsafe(self.queue.put(data), self.event_loop)

    async def loop(self, symbol):
        print("wait bin")
        async with self.bm.trade_socket(symbol) as ts:
            while True:
                try:
                    print("wait bin")
                    # self.logger.info("wait bin")
                    data = await asyncio.wait_for(ts.recv(), 5)
                    print("recieved bin")
                    # self.logger.info("recieved bin")
                    if data != None:
                        for callback in self.callbacks:
                            callback('binance-s', data)
                except Exception as e:
                    print("retry binance")
                    print(e)
                    # self.logger.warning("retry binance")
                    self.subscribe_futures(symbol)
                    break

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

    binance_api.subscribe_futures('ETHUSDT')
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
