import requests
import json
import asyncio
import websockets

import Crypto.PublicKey.RSA
import time
import numpy as np
import math
import sys

class TickSpreadApi:
    def __init__(self):
        self.last_id = int(time.time())
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
            print(r.text)
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
        self.last_id += 1
        order['client_order_id'] = self.last_id
        url = 'http://%s/api/v1/orders' % self.host
        try:
            r = requests.post(url, headers={"authorization": (
                "Bearer %s" % self.token)}, json=order, timeout=5.0)
            client_order_id = json.loads(r.text)['client_order_id']
        except:
            print(r.text)
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

class MarketMaker:
    def __init__(self, api, logname="mmaker.log"):
        self.api = api
        self.fair_price = 80200.0
        self.spread = 0.3
        self.num_orders = 4
        self.order_size = 5
        self.tick = 1
        self.leverage = 10
        self.symbol = 'BTC-PERP'
        self.buy_orders = []
        self.sel_orders = []
        self.top_buy_price = -1
        self.top_sel_price = -1
        self.logfile = open(logname, 'a')
        self.logfile.write("Starting\n")
        self.logfile.flush()
        api.on_message(self.callback)
    
    def update_fair_price(self):
        self.fair_price += np.random.normal() * 0.5
    
    def log(self, message):
        self.logfile.write(message)
        self.logfile.flush()
    
    def create_buy_order(self, price, amount, index=-1):
        self.log("->NEW bid %d @ %d" % (amount, price))
        clordid = self.api.create_order({
             "amount": amount,
             "price": price,
             "leverage": self.leverage,
             "symbol": self.symbol,
             "side": 'bid',
             "type": "limit"
        })
        self.log(" (%ld)\n" % clordid)
        self.buy_orders.insert(index, (price, clordid))
    
    def delete_buy_order(self, index):
        order_to_cancel = self.buy_orders[index]
        self.log("->CAN bid @ %d (%ld)\n" % (order_to_cancel[0], order_to_cancel[1]))
        self.api.delete_order(order_to_cancel[1])
        self.buy_orders.pop(index)

    def create_sel_order(self, price, amount, index=-1):
        self.log("->NEW ask %d @ %d" % (amount, price))
        clordid = self.api.create_order({
             "amount": amount,
             "price": price,
             "leverage": self.leverage,
             "symbol": self.symbol,
             "side": 'ask',
             "type": "limit"
        })
        
        self.log(" (%ld)\n" % clordid)
        self.sel_orders.insert(index, (price, clordid))

    def delete_sel_order(self, index):
        order_to_cancel = self.sel_orders[index]
        self.log("->CAN ask @ %d (%ld)\n" % (order_to_cancel[0], order_to_cancel[1]))
        self.api.delete_order(order_to_cancel[1])
        self.sel_orders.pop(index)
    
    def update_orders(self):
        new_top_buy_price = int(math.floor(self.fair_price - self.spread))
        new_top_sel_price = int(math.ceil(self.fair_price + self.spread))
        print("update_orders")
        
        if (not self.buy_orders):
            for i in range(self.num_orders):
                self.create_buy_order(new_top_buy_price - i * self.tick, self.order_size, i)
            self.top_buy_price = new_top_buy_price
        else:
            # Update Bids
            if (new_top_buy_price < self.top_buy_price):
                # Price has fallen, cancel first orders
                num_ticks_fall = int((self.top_buy_price - new_top_buy_price)/self.tick)
                num_orders_to_remove = min(self.num_orders, num_ticks_fall)
                for i in range(num_orders_to_remove):
                    self.delete_buy_order(i)
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
            self.top_buy_price = new_top_buy_price
        
        if (not self.sel_orders):
            for i in range(self.num_orders):
                self.create_sel_order(new_top_sel_price + i * self.tick, self.order_size, i)
            self.top_sel_price = new_top_sel_price
        else:
            # Update Asks
            if (new_top_sel_price > self.top_sel_price):
                # Price has risen, cancel first orders
                num_ticks_rise = int((new_top_sel_price - self.top_sel_price)/self.tick)
                num_orders_to_remove = min(self.num_orders, num_ticks_rise)
                for i in range(num_orders_to_remove):
                    self.delete_sel_order(i)
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
            self.top_sel_price = new_top_sel_price
        
        print(new_top_buy_price, new_top_sel_price)
    
    def callback(self, data):
        print("callback")
        if ("event" in data and data["event"] == "update"):
            self.update_fair_price()
            self.update_orders()
        else:
            print(data)

async def main():
    api = TickSpreadApi()

    # api.register("zxcv1", "zxcv1")
    login_status = api.login("zxcv1", "zxcv1")
    
    if (not login_status):
        asyncio.get_event_loop().stop()
        return 1;
    
    mmaker = MarketMaker(api)

    await api.connect()
    await api.subscribe("market_data", {"symbol": "BTC-PERP"})
    await api.subscribe("user_data", {"symbol": "BTC-PERP"})
    
    # order = api.create_order({
    #     "amount": 5,
    #     "price": 80000,
    #     "leverage": 10,
    #     "symbol": 'BTC-PERP',
    #     "side": 'bid',
    #     "type": limit
    # })
    # print(order)

    # api.delete_order(order['client_order_id'])

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()

