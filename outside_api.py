from typing import Optional, Dict, Any, List
import gzip
import requests
import json
import asyncio
import websockets
import logging

from requests import Request, Session, Response

import time
import math
import sys
import hmac

import queue
import urllib.parse
import traceback
from binance.client import AsyncClient
from binance import BinanceSocketManager, ThreadedWebsocketManager
from pythclient.pythaccounts import PythPriceAccount, PythPriceStatus
from pythclient.solana import SolanaClient, SolanaPublicKey, PYTHNET_HTTP_ENDPOINT, PYTHNET_WS_ENDPOINT


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
                    #print("wait bin")
                    # self.logger.info("wait bin")
                    data = await asyncio.wait_for(ts.recv(), 10)
                    #print("recieved bin")
                    # self.logger.info("recieved bin")
                    
                    if data != None:
                        for callback in self.callbacks:
                            callback('binance-s', data)
                except Exception as e:
                    print("retry binance")
                    traceback.print_stack()
                    print(e)
                    # self.logger.warning("retry binance")
                    self.subscribe_futures(symbol)
                    break

class KuCoinAPI:
    def __init__(self, logger=None, public_token=None):
        self.websocket_uri = f"wss://ws-api-spot.kucoin.com/?token={public_token}"
        self.logger = logger
        self.callbacks = []
        self.event_loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()

    def subscribe_index_price(self, symbol):
        self.event_loop.create_task(self.loop(symbol))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, symbol):
        while True:
            try:
                async with websockets.connect(self.websocket_uri) as websocket:
                    subscribe_message = {
                        "id": "1",
                        "type": "subscribe",
                        "topic": f"/indicator/index:{symbol}",
                        "response": True
                    }
                    await websocket.send(json.dumps(subscribe_message))

                    while True:
                        response = await websocket.recv()
                        data = json.loads(response)

                        if data["type"] == "message" and data["subject"] == "tick":
                            for callback in self.callbacks:
                                callback('kucoin', data)
            except Exception as e:
                if self.logger:
                    self.logger.warning("retry kucoin")
                print("retry kucoin")
                print(e)
                self.subscribe_index_price(symbol)

class PythXauAPI:
    def __init__(self, logger=None, public_token=None):
        self.logger = logger
        self.callbacks = []
        self.event_loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()

    def subscribe_index_price(self, symbol):
        self.event_loop.create_task(self.loop(symbol))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def get_gold_price(self):
        # pythnet GOLD/USD price account key (available on pyth.network website)
        account_key = SolanaPublicKey("8y3WWjvmSmVGWVKH1rCA7VTRmuU7QbJ9axafSsBX5FcD")
        solana_client = SolanaClient(endpoint=PYTHNET_HTTP_ENDPOINT, ws_endpoint=PYTHNET_WS_ENDPOINT)
        price: PythPriceAccount = PythPriceAccount(account_key, solana_client)

        await price.update()
        data =  { "status": "fail" }

        price_status = price.aggregate_price_status
        if price_status == PythPriceStatus.TRADING:
            # Sample output: "DOGE/USD is 0.141455 ± 7.4e-05"
            #print("GOLD/USD is", price.aggregate_price, "±", price.aggregate_price_confidence_interval)
            data =  { "status": "ok", "p": price.aggregate_price, "confidence":price.aggregate_price_confidence_interval }
        else:
            print("Price is not valid now. Status is", price_status)

        await solana_client.close()
        return data

    async def loop(self, symbol):
        while True:
            try:
                while True:
                    data = await self.get_gold_price()

                    for callback in self.callbacks:
                        callback('pyth', data)
            except Exception as e:
                if self.logger:
                    self.logger.warning("retry Pyth")
                print("retry Pyth")
                print(e)
                self.subscribe_index_price(symbol)

def test_callback(source, raw_data):
    timestamp = time.time()
    print("post test_callback", raw_data)
    logging.info("%f %-10s: %s" % (timestamp, source, raw_data))


async def main():
    #binance_api = BinanceAPI()
    pyth_xau_api = PythXauAPI()

    logging.basicConfig(level=logging.INFO, filename="dump.log")

    #binance_api.subscribe_futures('ETHUSDT')
    #binance_api.on_message(test_callback)

    # Set up and test PythXauAPI
    pyth_xau_api.subscribe_index_price('XAU/USD')
    pyth_xau_api.on_message(test_callback)

    # Run for a specific duration to test
    await asyncio.sleep(60)  # Run for 60 seconds

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()