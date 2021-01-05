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

logging.basicConfig(level=logging.INFO, filename="bot.log")

class TickSpreadApi:
    def __init__(self):
        self.last_id = int(time.time()*100)
        self.host = 'localhost:4000'

    def login(self, username, password):
        payload = {"username": username, "password_hash": password}
        url = 'http://%s/api/v1/accounts/login' % self.host
        r = requests.post(url, json=payload, timeout=5.0)
        try:
            data = json.loads(r.text)
            self.token = data["token"]
            self.callbacks = []
            self.websocket = None
            status = True
        except:
            logging.error(r.text)
            status = False
        return status
    
    def register(self, username, password):
        authentication_method = {"type": "userpass",
                                 "key": username, "value": password}
        new_user = {"role": "admin",
                    "authentication_methods": [authentication_method]}
        payload = {"users": [new_user]}

        url = 'http://%s/api/v1/accounts' % self.host
        requests.post(url, json=payload, timeout=5.0)

    def create_order(self, order):
        self.last_id += 100
        order['client_order_id'] = self.last_id
        url = 'http://%s/api/v1/orders' % self.host
        try:
            #logging.info(order)
            r = requests.post(url, headers={"authorization": (
                "Bearer %s" % self.token)}, json=order, timeout=5.0)
            client_order_id = json.loads(r.text)['client_order_id']
        except:
            logging.error(r.text)
            sys.exit(1)
        return client_order_id

    def delete_order(self, client_order_id):
        url = 'http://%s/api/v1/orders/%s' % (self.host, client_order_id)
        r = requests.delete(url, headers={"authorization": (
            "Bearer %s" % self.token)}, timeout=5.0)
        return json.loads(r.text)

    async def connect(self):
        self.websocket = await websockets.connect("ws://%s/realtime" % self.host, ping_interval=None)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic, arguments):
        data = {
            "topic": topic,
            "event": "subscribe",
            "payload": arguments,
            "authorization": "Bearer %s" % self.token
        }

        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()
            message = json.loads(message)
            for callback in self.callbacks:
                callback(message)

class FTXAPI:
    def __init__(self):
        self.host = "wss://ftx.com/ws/"
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
    def __init__(self):
        self.host = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
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
    def __init__(self):
        self.host = "wss://stream.bybit.com/realtime"
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
    def __init__(self):
        self.host = "wss://api.btcgateway.pro/swap-ws"
        self.host = "wss://api.huobi.pro/ws"
        self.host = "ws://api.hbdm.vn/linear-swap-ws" 	# Working
        self.host = "wss://api.hbdm.com/swap-ws"
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
    def __init__(self):
        self.spot_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
        self.usdt_futures_url = "wss://fstream.binance.com/ws/"
        self.coin_futures_url = "wss://dstream.binance.com/ws/"
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
 
class MarketMaker:
    def __init__(self, api, logname="mmaker.log"):
        self.api = api
        
        self.bid_price = None
        self.ask_price = None
        
        self.num_orders = 6
        self.order_size = 5
        self.tick = 5
        self.leverage = 10
        self.symbol = 'BTC-PERP'
        self.buy_orders = []
        self.sel_orders = []
        self.top_buy_price = -1
        self.top_sel_price = -1
        self.top_buy_increase = 0
        self.top_sel_decrease = 0
        self.real = True
        
    def update_fair_price(self):
        self.fair_price += np.random.normal() * 0.5
    
    def create_buy_order(self, price, amount, index=-1):
        logging.info("->NEW bid %d @ %d" % (amount, price))
        clordid = 0
        if (self.real):
            clordid = self.api.create_order({
                 "amount": amount,
                 "price": price,
                 "leverage": self.leverage,
                 "symbol": self.symbol,
                 "side": 'bid',
                 "type": "limit"
            })
        self.buy_orders.insert(index, (price, clordid))
    
    def delete_buy_order(self, index):
        order_to_cancel = self.buy_orders[index]
        logging.info("->CAN bid   @ %d (%ld)" % (order_to_cancel[0], order_to_cancel[1]))
        if (self.real):
            self.api.delete_order(order_to_cancel[1])
        self.buy_orders.pop(index)

    def create_sel_order(self, price, amount, index=-1):
        logging.info("->NEW ask %d @ %d" % (amount, price))
        clordid = 0
        if (self.real):
            clordid = self.api.create_order({
                 "amount": amount,
                 "price": price,
                 "leverage": self.leverage,
                 "symbol": self.symbol,
                 "side": 'ask',
                 "type": "limit"
            })
        self.sel_orders.insert(index, (price, clordid))

    def delete_sel_order(self, index):
        order_to_cancel = self.sel_orders[index]
        logging.info("->CAN ask   @ %d (%ld)" % (order_to_cancel[0], order_to_cancel[1]))
        if (self.real):
            self.api.delete_order(order_to_cancel[1])
        self.sel_orders.pop(index)
    
    def update_orders(self, timestamp, new_top_buy_price, new_top_sel_price):
        #new_top_buy_price = int(math.floor(self.fair_price - self.spread))
        #new_top_sel_price = int(math.ceil(self.fair_price + self.spread))
        #print("update_orders")
        
        if (not self.buy_orders):
            for i in range(self.num_orders):
                self.create_buy_order(new_top_buy_price - i * self.tick, self.order_size, i)
            self.top_buy_price = new_top_buy_price
            self.top_buy_increase = timestamp
        else:
            # Update Bids
            if (new_top_buy_price < self.top_buy_price):
                # Price has fallen, cancel first orders
                num_ticks_fall = int((self.top_buy_price - new_top_buy_price)/self.tick)
                num_orders_to_remove = min(self.num_orders, num_ticks_fall)
                #print(num_orders_to_remove, self.buy_orders)
                for i in range(num_orders_to_remove):
                    self.delete_buy_order(0)
                # Create new orders at the bottom
                for i in range(len(self.buy_orders), self.num_orders):
                    self.create_buy_order(new_top_buy_price - i * self.tick, self.order_size, i)
            elif (new_top_buy_price > self.top_buy_price):
                # Price has risen, create new orders at the top
                num_ticks_rise = int((new_top_buy_price - self.top_buy_price)/self.tick)
                num_orders_to_create = min(self.num_orders, num_ticks_rise)
                for i in range(num_orders_to_create):
                    self.create_buy_order(new_top_buy_price - i * self.tick, self.order_size, i)
                # Cancel orders at the bottom
                for i in range(len(self.buy_orders)-1, self.num_orders-1, -1):
                    self.delete_buy_order(i)
                self.top_buy_increase = timestamp
            self.top_buy_price = new_top_buy_price
        
        if (not self.sel_orders):
            for i in range(self.num_orders):
                self.create_sel_order(new_top_sel_price + i * self.tick, self.order_size, i)
            self.top_sel_price = new_top_sel_price
            self.top_sel_decrease = timestamp
        else:
            # Update Asks
            if (new_top_sel_price > self.top_sel_price):
                # Price has risen, cancel first orders
                num_ticks_rise = int((new_top_sel_price - self.top_sel_price)/self.tick)
                num_orders_to_remove = min(self.num_orders, num_ticks_rise)
                #print(num_orders_to_remove, self.sel_orders)
                for i in range(num_orders_to_remove):
                    self.delete_sel_order(0)
                # Create new orders at the bottom
                for i in range(len(self.sel_orders), self.num_orders):
                    self.create_sel_order(new_top_sel_price + i * self.tick, self.order_size, i)
            elif (new_top_sel_price < self.top_sel_price):
                # Price has fallen, create new orders at the top
                num_ticks_fall = int((self.top_sel_price - new_top_sel_price)/self.tick)
                num_orders_to_create = min(self.num_orders, num_ticks_fall)
                for i in range(num_orders_to_create):
                    self.create_sel_order(new_top_sel_price + i * self.tick, self.order_size, i)
                # Cancel orders at the bottom
                for i in range(len(self.sel_orders)-1, self.num_orders-1, -1):
                    self.delete_sel_order(i)
                self.top_sel_decrease = timestamp
            self.top_sel_price = new_top_sel_price
        
        #print(new_top_buy_price, new_top_sel_price)
    
    def callback(self, source, raw_data):
        ts = time.time()
        #data = json.loads(raw_data)
        #transformed_data = raw_data.replace(", {", "\n{").replace(",{", "\n{")
            
        if (source == 'binance-f'):
            #logging.info("%f %-10s: %s" % (ts, source, raw_data))
            data = json.loads(raw_data)
            if ("p" in data):
                new_price = float(data["p"])
                spread = 0.00002
                new_top_buy_price = math.floor(new_price*(1-spread)/self.tick)*self.tick
                new_top_sel_price = math.ceil(new_price*(1+spread)/self.tick)*self.tick
                
                #print(new_top_buy_price, new_top_sel_price, end=' ')
                if (new_top_buy_price > self.top_buy_price and ts < self.top_buy_increase + 1.0):
                    new_top_buy_price = self.top_buy_price
                
                if (new_top_sel_price < self.top_sel_price and ts < self.top_sel_decrease + 1.0):
                    new_top_sel_price = self.top_sel_price
                
                #print(new_top_buy_price, new_top_sel_price)
                self.update_orders(ts, new_top_buy_price, new_top_sel_price)

    
    '''
    def callback(self, data):
        return;
        if ("event" in data and data["event"] == "update"):
            self.update_orders()
        else:
            print(data)
    
    def ftx_callback(self, data):
        EXTRA_SPREAD = 0.00002
        if ('channel' in data and data['channel'] == 'ticker' and 'data' in data):
            ticker = data['data']
            new_top_buy_price = math.floor(ticker['bid']*(1-EXTRA_SPREAD))
            new_top_sel_price = math.ceil(ticker['ask']*(1+EXTRA_SPREAD))
            logging.info("%d, %d" % (new_top_buy_price, new_top_sel_price))
            #self.update_orders(new_top_buy_price, new_top_sel_price)
    
    def binance_callback(self, data):
        logging.info(data)
    '''

async def main():
    api = TickSpreadApi()
    #api.register("maker@tickspread.com", "maker")
    login_status = api.login("maker@tickspread.com", "maker")
    if (not login_status):
        asyncio.get_event_loop().stop()
        return 1;

    mmaker = MarketMaker(api)

    bybit_api = ByBitAPI()
    ftx_api = FTXAPI()
    binance_api = BinanceAPI()
    bitmex_api = BitMEXAPI()
    huobi_api = HuobiAPI()
    
    #await api.connect()
    #await api.subscribe("market_data", {"symbol": "BTC-PERP"})
    #await api.subscribe("user_data", {"symbol": "BTC-PERP"})

    #await bybit_api.connect()
    #await bybit_api.subscribe()
    #bybit_api.on_message(mmaker.callback)

    await binance_api.connect()
    await binance_api.subscribe_spot(["btcusdt@trade"])
    await binance_api.subscribe_usdt_futures(["btcusdt@trade"])
    await binance_api.subscribe_coin_futures(["btcusd_perp@trade"])
    binance_api.on_message(mmaker.callback)
    
    await ftx_api.connect()
    #await ftx_api.subscribe('ticker')
    #await ftx_api.subscribe('orderbook')
    await ftx_api.subscribe('trades')
    #logging.info("Done")
    ftx_api.on_message(mmaker.callback)
    
    await bitmex_api.connect()
    bitmex_api.on_message(mmaker.callback)
    
    await huobi_api.connect()
    await huobi_api.subscribe()
    huobi_api.on_message(mmaker.callback)
    


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()
