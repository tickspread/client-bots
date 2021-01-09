import requests
import json
import asyncio
import websockets
import logging

import Crypto.PublicKey.RSA
import time
import numpy as np
import math
import sys


class FTXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://ftx.com/ws/"
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
                callback("ftx", message)

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

            

import gzip

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
    def __init__(self, logger=logging.getLogger()):
        self.spot_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
        self.usdt_futures_url = "wss://fstream.binance.com/ws/"
        self.coin_futures_url = "wss://dstream.binance.com/ws/"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket_spot = await websockets.connect(self.spot_url)
        self.websocket_usdt_futures = await websockets.connect(self.usdt_futures_url)
        self.websocket_coin_futures = await websockets.connect(self.coin_futures_url)
        asyncio.get_event_loop().create_task(self.loop(self.websocket_spot, "binance-s"))
        asyncio.get_event_loop().create_task(self.loop(self.websocket_usdt_futures, "binance-f"))
        asyncio.get_event_loop().create_task(self.loop(self.websocket_coin_futures, "binance-d"))

    async def subscribe_spot(self, topics):
        data = {"method": "SUBSCRIBE", "params": topics, "id": 1}
        await self.websocket_spot.send(json.dumps(data))

    async def subscribe_usdt_futures(self, topics):
        data = {"method": "SUBSCRIBE", "params": topics, "id": 1}
        await self.websocket_usdt_futures.send(json.dumps(data))
        
    async def subscribe_coin_futures(self, topics):
        data = {"method": "SUBSCRIBE", "params": topics, "id": 1}
        await self.websocket_coin_futures.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)
    
    async def loop(self, websocket, source_name):
        while True:
            message = await websocket.recv()
            #message = json.loads(message)
            for callback in self.callbacks:
                callback(source_name, message)

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

    await binance_api.connect()
    await binance_api.subscribe_spot(["btcusdt@trade"])
    await binance_api.subscribe_usdt_futures(["btcusdt@trade"])
    await binance_api.subscribe_coin_futures(["btcusd_perp@trade"])
    binance_api.on_message(test_callback)
    
    await ftx_api.connect()
    #await ftx_api.subscribe('ticker')
    #await ftx_api.subscribe('orderbook')
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
