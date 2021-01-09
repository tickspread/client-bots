import requests
import json
import asyncio
import websockets
import logging

import time
import numpy as np
import math
import sys

from tickspread_api import TickSpreadAPI
from outside_api import ByBitAPI, FTXAPI, BinanceAPI, BitMEXAPI, HuobiAPI

logging.basicConfig(level=logging.INFO, filename="bot.log")

class Side(Enum):
    BID = 1
    ASK = 2

class OrderState(Enum):
    EMPTY = 0
    PENDING_NEW = 1
    PENDING_FILL = 2
    PENDING_CANCEL = 3

class Order:
    def __init__(self, side):
        self.clordid = None
        self.side = side
        self.price = None
        self.total_amount = 0
        self.amount_left = 0
        self.state = OrderState.EMPTY
    
    def new(self, clordid, amount, price):
        assert(self.state == OrderState.EMPTY)
        self.state = OrderState.PENDING_NEW
        self.clordid = clordid
        self.total_amount = amount
        self.amount_left = amount
        self.price = price
    
    def cancel(self):
        assert(self.state == OrderState.PENDING_NEW
            or self.state == OrderState.PENDING_FILL)
        self.state = OrderState.PENDING_CANCEL
    
    def exec_new(self):
        assert(self.state == OrderState.PENDING_NEW
            or self.state == OrderState.PENDING_CANCEL)
        
        if (self.state == OrderState.PENDING_NEW):
            self.state = OrderState.PENDING_FILL
    
    def exec_trade(self, execution_amount):
        assert(self.state == OrderState.PENDING_FILL
            or self.state == OrderState.PENDING_CANCEL)
        assert(execution_amount <= self.amount_left)
        self.amount_left -= execution_amount
        
        if (self.amount_left == 0):
            self.state = OrderState.EMPTY
            self.total_amount = 0
            self.clordid = None
            self.price = None
    
    def exec_cancel(self):
        assert(self.state == OrderState.PENDING_CANCEL)
        self.state = OrderState.EMPTY
        self.total_amount = 0
        self.amount_left = 0
        self.clordid = None
        self.price = None

class MarketMaker:
    def __init__(self, api, logger=logging.getLogger()):
        self.api = api
        self.logger = logger
        
        self.bid_price = None
        self.ask_price = None
        
        self.num_orders = 6
        self.order_size = 5
        self.tick = 2
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
        
        '''
        if (source == 'binance-f'):
        '''
        if (source == 'ftx'):
            #logging.info("%f %-10s: %s" % (ts, source, raw_data))
            data = json.loads(raw_data)
            new_price = None
            if ("p" in data):
                new_price = float(data["p"])

            if ("data" in data):
                for trade_line in data["data"]:
                    if ("price" in trade_line):
                        new_price = trade_line["price"]
            
            if (new_price != None):
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
    api = TickSpreadAPI()
    #api.register("maker@tickspread.com", "maker")
    login_status = api.login("kelvin@tickspread.com", "kelvin")
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

    #await binance_api.connect()
    #await binance_api.subscribe_spot(["btcusdt@trade"])
    #await binance_api.subscribe_usdt_futures(["btcusdt@trade"])
    #await binance_api.subscribe_coin_futures(["btcusd_perp@trade"])
    #binance_api.on_message(mmaker.callback)
    
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
