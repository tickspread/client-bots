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

class FTXAPI:
    def __init__(self, auth=None, logger=logging.getLogger()):
        self.endpoint = 'https://ftx.com/api/'
        self.host = "wss://ftx.com/ws/"
        self.logger = logger
        self.callbacks = []
        self.last_ping = 0
        
        self.topics = []
        self.has_login = False
        
        self._session = Session()
        self.auth = auth
        if self.auth:
            self._api_key = self.auth.get("FTX_API_KEY")
            self._api_secret = self.auth.get("FTX_API_SECRET")
            self._subaccount_name = self.auth.get("FTX_SUBACCOUNT")
            
            assert(self._api_secret)
            assert(self._api_key)
            assert(self._subaccount_name)
        else:
            # assert(False)
            pass

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                print(data)
                raise Exception(data['error'])
            if type(data['result']) != list: 
                data['result']['success'] = data['success']
            return data['result']

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        count = 0
        while count < 5:
            try:
                request = Request(method, self.endpoint + path, **kwargs)
                self._sign_request(request)
                response = self._session.send(request.prepare())
                return self._process_response(response)
            except Exception as e:
                print("FTX Client request failed, retrying ", e, method, path)
                time.sleep(5)
                count += 1
        raise {'error':'Failed FTX request after 5 retries'}
    
    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)


    def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit',
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None, reject_after_ts: float = None) -> dict:
        return self._post('orders', {
            'market': market,
            'side': side,
            'price': price,
            'size': size,
            'type': type,
            'reduceOnly': reduce_only,
            'ioc': ioc,
            'postOnly': post_only,
            'clientId': client_id,
            'rejectAfterTs': reject_after_ts
        })

    def get_positions(self, show_avg_price: bool = False) -> List[dict]:
        return self._get('positions', {'showAvgPrice': show_avg_price})

    async def connect(self):
        self.websocket = await websockets.connect(self.host, ping_interval=None)
    
    async def login(self):
        assert(self._api_secret)
        assert(self._api_key)
        assert(self._subaccount_name)
        self.has_login = True
        
        ts = int(time.time() * 1000)
        login_msg = {'op': 'login', 'args': {
            'key': self._api_key,
            'sign': hmac.new(
                self._api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
            'time': ts,
            'subaccount': self._subaccount_name,
        }}
        await self.websocket.send(json.dumps(login_msg))

    async def subscribe(self, topic, market):
        self.topics.append((topic, market))
        data = {'op': 'subscribe', 'channel': topic, 'market': market}
        await self.websocket.send(json.dumps(data))
        

    def on_message(self, callback):
        self.callbacks.append(callback)
        
    async def start_loop(self):
        asyncio.get_event_loop().create_task(self.loop())

    async def reconnect(self):
        try:
            time.sleep(1)
            print("FTX reconnect")
            
            print("Try connect")
            await self.connect()
            print("Try Login")
            if (self.has_login):
                await self.login()
            print("Try Subscribe, self.topics = ", self.topics)
            topics = self.topics
            self.topics = []
            for topic, market in topics:
                print("Try sub: ", topic, flush=True)
                await self.subscribe(topic, market)
                time.sleep(1)
                print("end 1")
            print("end 2")
            return True
        except Exception as e:
            print("Reconnect Exception: ", e)
            # or
            print(sys.exc_info())
            return False

    async def loop(self):
        while True:
            try:
                print("Wait")
                message = await asyncio.wait_for(self.websocket.recv(), timeout=10)

                print("Received")
                #message = json.loads(message)
                for callback in self.callbacks:
                    callback("ftx", message)
            except Exception as e:
                print("FTX Loop Exception: ", e)
                attempts = 0
                await self.websocket.close()
                while await self.reconnect() != True:
                    attempts += 1
                    print("Reconnection Failed Attempt Number ", attempts)
                await self.start_loop()
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
                    data = await asyncio.wait_for(ts.recv(), 10)
                    print("recieved bin")
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
def test_callback(source, raw_data):
    timestamp = time.time()
    print("post test_callback", raw_data)
    logging.info("%f %-10s: %s" % (timestamp, source, raw_data))


async def main():
    # bybit_api = ByBitAPI()
    ftx_api = FTXAPI()
    # binance_api = BinanceAPI()
    # bitmex_api = BitMEXAPI()
    # huobi_api = HuobiAPI()

    # logging.basicConfig(level=logging.INFO, filename="dump.log")

    # await bybit_api.connect()
    # await bybit_api.subscribe()
    # bybit_api.on_message(test_callback)

    # binance_api.subscribe_futures('ETHUSDT')
    # binance_api.on_message(test_callback)
    # binance_api.stop()
    print("pre ftx_api.connect()")
    await ftx_api.connect()
    # await ftx_api.subscribe('ticker')
    # await ftx_api.subscribe('orderbook')
    print("pre await ftx_api.subscribe('trades')")
    await ftx_api.subscribe('trades', 'ETH-PERP')
    print("pre ftx_api.on_message(test_callback)")
    ftx_api.on_message(test_callback)
    print("pre ftx_api.start_loop()")
    await ftx_api.start_loop()
    print("post ftx_api.start_loop()")

    # await bitmex_api.connect()
    # bitmex_api.on_message(test_callback)

    # await huobi_api.connect()
    # await huobi_api.subscribe()
    # huobi_api.on_message(test_callback)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()
