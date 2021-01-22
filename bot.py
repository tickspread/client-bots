# -*- coding: utf-8 -*-
"""Example TickSpread Bot

This module gives an example market-making bot that listens to market-data
feeds from external exchanges (Binance, FTX, Huobi, BitMEX and Bybit) and
puts orders at TickSpread.

The bot controls the state for each order, listening to update at the
user-data feed. It respects a maximum position and attemps to maintain a target
number of open orders, subject to a maximum.

All orders sent and updates received are logged at "bot.log".

Example:
    To run the bot::

        $ python3 bot.py

Use this bot are your own risk! No guarantees -- it probably has bugs!

Todo:
    * External real-time parameter changes

"""

import requests
import json
import asyncio
import websockets
import logging
from enum import Enum

import time
import numpy as np
import math
import sys

from tickspread_api import TickSpreadAPI
from outside_api import ByBitAPI, FTXAPI, BinanceAPI, BitMEXAPI, HuobiAPI

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    filename="bot.log")

class Side(Enum):
    BID = 1
    ASK = 2

def side_to_str(side):
    if (side == Side.BID):
        return "bid"
    elif (side == Side.ASK):
        return "ask"
    else:
        return "bug"


class OrderState(Enum):
    EMPTY = 0
    PENDING = 1
    ACKED = 2
    MAKER = 3
    ACTIVE = 4

def order_state_to_str(state):
    if (state == OrderState.EMPTY):
        return "   "
    elif (state == OrderState.PENDING):
        return "pen"
    elif (state == OrderState.ACKED):
        return "ack"
    elif (state == OrderState.MAKER):
        return "mak"
    elif (state == OrderState.ACTIVE):
        return "act"
    else:
        return "bug"


class CancelState(Enum):
    NORMAL = 0
    PENDING = 1


class Order:
    def __init__(self, side, logger):
        self.clordid = None
        self.side = side
        self.price = None
        self.total_amount = 0
        self.amount_left = 0
        self.state = OrderState.EMPTY
        self.cancel = CancelState.NORMAL
        self.logger = logger

    def __str__(self):
        if (self.state == OrderState.EMPTY):
            return ""
        else:
            return "%s %d/%d @ %d (%d) [%s]" % (
                side_to_str(self.side), self.amount_left, self.total_amount,
                self.price, self.clordid, order_state_to_str(self.state))


class MarketMakerSide:
    def __init__(self, parent, *, side, target_num_orders, max_orders,
                 order_size, available_limit, tick_jump):
        self.parent = parent
        self.side = side
        self.target_num_orders = target_num_orders
        self.max_orders = max_orders
        self.order_size = order_size
        self.available_limit = available_limit
        self.tick_jump = tick_jump

        self.top_order = 0
        self.top_price = None
        self.orders = []
        for i in range(self.max_orders):
            self.orders.append(Order(self.side, self.parent.logger))

    def debug_orders(self):
        for i in range(self.max_orders):
            if (self.orders[i].state != OrderState.EMPTY):
                self.parent.logger.info("%d: %s", i, self.orders[i])



    def set_new_price(self, new_price):
        if (self.side == Side.BID):
            new_top_price = math.floor(
                new_price / self.tick_jump) * self.tick_jump
        else:
            new_top_price = math.ceil(
                new_price / self.tick_jump) * self.tick_jump
        self.old_top_price = self.top_price
        self.top_price = new_top_price
        self.old_top_order = self.top_order
        if (self.old_top_price):
            price_diff = self.top_price - self.old_top_price
            if (self.side == Side.BID):
                steps_diff = -int(price_diff / self.tick_jump)
            else:
                steps_diff = +int(price_diff / self.tick_jump)
            self.top_order = self.old_top_order + steps_diff
        self.parent.logger.debug(
            "%s - top: %d => %d" %
            (side_to_str(self.side), self.old_top_order, self.top_order))

    def maybe_cancel_top_orders(self):
        self.parent.logger.debug("maybe_cancel_top_orders: %d" % self.top_price)
        if (self.top_order > self.old_top_order):
            orders_to_remove = min(self.top_order - self.old_top_order,
                                   self.target_num_orders)
            for i in range(orders_to_remove):
                index = (self.old_top_order + i) % (self.max_orders)
                order = self.orders[index]
                if (order.state != OrderState.EMPTY
                        and order.cancel == CancelState.NORMAL):
                    self.parent.send_cancel(order)

    def recalculate_top_orders(self):
        self.parent.logger.debug("recalculate_top_orders: %d", self.top_price)
        self.debug_orders()
        initial_price = self.top_price
        if (self.side == Side.BID):
            price_increment = -self.tick_jump
        else:
            price_increment = +self.tick_jump
        price = initial_price
        for i in range(self.target_num_orders):
            self.parent.logger.info("index = %d (%d)",
                                     self.top_order + i,
                                     (self.top_order + i) % self.max_orders)
            if (self.top_order + i >=
                    self.old_top_order + self.target_num_orders):
                self.parent.logger.info("breaking at %d",
                                        self.top_order + i)
                # Send new orders at the bottom later
                break
            index = (self.top_order + i) % (self.max_orders)
            order = self.orders[index]
            if (order.state == OrderState.EMPTY):
                size = min(self.order_size, self.available_limit)
                if (size > 0):
                    self.available_limit -= size
                    self.parent.send_new(order, size, price)
            price += price_increment

    def recalculate_bottom_orders(self):
        self.parent.logger.info("recalculate_bottom_orders: %d",
                                self.top_price)
        if (self.top_order > self.old_top_order):
            initial_price = self.top_price
            if (self.side == Side.BID):
                price_increment = -self.tick_jump
            else:
                price_increment = +self.tick_jump

            price = initial_price + self.target_num_orders * price_increment
            for i in range(
                    min(self.top_order - self.old_top_order,
                        self.target_num_orders)):
                index = (self.old_top_order + self.target_num_orders +
                         i) % (self.max_orders)
                order = self.orders[index]

                if (order.state == OrderState.EMPTY):
                    size = min(self.order_size, self.available_limit)
                    if (size > 0):
                        self.available_limit -= size
                        self.parent.send_new(order, size, price)

                if (order.state != OrderState.EMPTY
                        and order.cancel == CancelState.NORMAL):
                    if (price != order.price):
                        self.parent.send_cancel(order)

                price += price_increment

    def maybe_cancel_bottom_orders(self):
        self.parent.logger.info("maybe_cancel_bottom_orders: %d",
                                self.top_price)
        self.debug_orders()

        for i in range(self.target_num_orders, self.max_orders):
            index = (self.top_order + i) % (self.max_orders)
            order = self.orders[index]

            if (order.state != OrderState.EMPTY
                    and order.cancel == CancelState.NORMAL):
                self.parent.send_cancel(order)


class MarketMaker:
    def __init__(self, api, *, logger=logging.getLogger(),\
                    name="bot_example", version="0.0", \
                    orders_per_side=8, max_position=400, tick_jump=10, order_size=5, leverage=10):
        # System
        self.api = api
        self.logger = logger
        self.name = name
        self.version = version

        # Structure
        self.bids = MarketMakerSide(self, side=Side.BID, \
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    order_size=order_size, available_limit=max_position, tick_jump=tick_jump)
        self.asks = MarketMakerSide(self, side=Side.ASK, \
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    order_size=order_size, available_limit=max_position, tick_jump=tick_jump)

        # Position and PnL
        self.position = 0
        #self.position_total_price = 0
        #self.gross_profit = 0
        #self.fees_paid = 0

        # Parameters
        self.tick_jump = tick_jump
        self.order_size = order_size
        self.leverage = leverage
        self.symbol = "BTC-PERP"

        # State
        self.real = True
        self.active = False
        self.fair_price = None
        self.spread = None

    def send_new(self, order, amount, price):
        clordid = self.api.get_next_clordid()
        logging.info(
            "->NEW %s %d @ %d (%d)" %
            (side_to_str(order.side), amount, price, clordid))
        if (self.real):
            self.register_new(order, clordid, amount, price)
            self.api.create_order(amount=amount,
                                  price=price,
                                  leverage=self.leverage,
                                  symbol=self.symbol,
                                  side=side_to_str(order.side),
                                  async=True)

    def send_cancel(self, order):
        logging.info(
            "->CAN %s %d @ %d (%d)" % (side_to_str(
                order.side), order.amount_left, order.price, order.clordid))
        if (self.real):
            self.register_cancel(order)
            self.api.delete_order(order.clordid,async=True)

    def register_new(self, order, clordid, amount, price):
        assert (order.state == OrderState.EMPTY)
        order.state = OrderState.PENDING
        order.cancel = CancelState.NORMAL
        order.clordid = clordid
        order.total_amount = amount
        order.amount_left = amount
        order.price = price

    def register_cancel(self, order):
        assert (order.cancel == CancelState.NORMAL)
        order.cancel = CancelState.PENDING

    def exec_ack(self, order):
        if (order.state != OrderState.PENDING):
            self.logger.warning(
                "Received acknowledge, but order %d is in state %s",
                order.clordid, order_state_to_str(order.state))
        order.state = OrderState.ACKED

    def exec_maker(self, order):
        if (order.state != OrderState.ACKED):
            self.logger.warning(
                "Received maker_order, but order %d is in state %s",
                order.clordid, order_state_to_str(order.state))
        order.state = OrderState.MAKER

    def exec_remove(self, order):
        if (order.cancel != CancelState.PENDING):
            self.logger.warning("Received unexpected remove_order in id %d",
                                order.clordid)
        if (order.side == Side.BID):
            self.bids.available_limit += order.amount_left
        else:
            self.asks.available_limit += order.amount_left
        # Reset order
        order.state = OrderState.EMPTY
        order.cancel = CancelState.NORMAL
        order.total_amount = 0
        order.amount_left = 0
        order.clordid = None
        order.price = None
    
    def find_order_by_clordid(self, clordid):
        for order in self.bids.orders:
            if (order.clordid == clordid):
                assert (order.side == Side.BID)
                return order
        for order in self.asks.orders:
            if (order.clordid == clordid):
                assert (order.side == Side.ASK)
                return order
        return None

    def receive_exec(self, event, clordid):
        order = self.find_order_by_clordid(clordid)
        if (not order):
            logging.warning("Received exec %s for unknown order: %d",
                            event, clordid)
            return
        if (event == "acknowledge_order"):
            self.exec_ack(order)
        elif (event == "maker_order"):
            self.exec_maker(order)
        elif (event == "delete_order"):
            self.exec_remove(order)
        else:
            self.logger.warning("Order %d received unknown event %s",
                                order.clordid, event)

    def _trade(self, order, execution_amount):
        if (execution_amount > order.amount_left):
            self.logger.error(
                "Received trade with execution_amount %d in order %d, but order has amount_left = %d"
                % (execution_amount, order.clordid, order.amount_left))
            sys.exit(1)
        # Update Order
        order.amount_left -= execution_amount
        if (order.amount_left == 0):
            order.state = OrderState.EMPTY
            order.cancel = CancelState.NORMAL
            order.total_amount = 0
            order.clordid = None
            order.price = None
        # Update Position
        if (order.side == Side.BID):
            self.position += execution_amount
            self.asks.available_limit += execution_amount
        else:
            self.position -= execution_amount
            self.bids.available_limit += execution_amount

    def receive_exec_trade(self, event, clordid, execution_amount):
        order = self.find_order_by_clordid(clordid)
        if (not order):
            logging.warning("Received exec trade %s for unknown order: %d",
                            event, clordid)
            return
        if (event == "maker_trade"):
            if (order.state != OrderState.MAKER):
                self.logger.warning(
                    "Received maker_trade, but order %d is in state %s",
                    order.clordid, order_state_to_str(order.state))
            self._trade(order, execution_amount)
        elif (event == "taker_trade"):
            if (order.state != OrderState.ACKED):
                self.logger.warning(
                    "Received taker_trade, but order %d is in state %s",
                    order.clordid, order_state_to_str(order.state))
            self._trade(order, execution_amount)
        else:
            logging.warning("Order %d received unknwon trade event %s",
                        order.clordid, event)

    def update_orders(self):
        assert (self.active)

        self.bids.set_new_price(self.fair_price - self.spread)
        self.asks.set_new_price(self.fair_price + self.spread)

        # When price falls, cancel top bids. When price rises, cancel top asks.
        self.bids.maybe_cancel_top_orders()
        self.asks.maybe_cancel_top_orders()

        # When price falls, send new top asks. When price rises, send new top bids.
        self.bids.recalculate_top_orders()
        self.asks.recalculate_top_orders()

        # When price falls, send new bottom bids to maintain the desired number of orders. When price rises, send new bottom asks
        self.bids.recalculate_bottom_orders()
        self.asks.recalculate_bottom_orders()

        # When price falls, cancel bottom asks to maintain the desired number of orders. When price rises, cancel bottom bids.
        self.bids.maybe_cancel_bottom_orders()
        self.asks.maybe_cancel_bottom_orders()

    def tickspread_callback(self, data):
        if (not 'event' in data):
            logging.warning("No 'event' in TickSpread message")
            return
        if (not 'payload' in data):
            logging.warning("No 'payload' in TickSpread message")
            return
        event = data['event']
        payload = data['payload']

        if (event == "update"):
            pass
        elif (event == "acknowledge_order" or event == "maker_order"
              or event == "delete_order"):
            if (not 'client_order_id' in payload):
                logging.warning(
                    "No 'client_order_id' in TickSpread %s payload", event)
                return
            clordid = int(payload['client_order_id'])
            print("clordid = %d" % clordid)
            self.receive_exec(event, clordid)
        elif (event == "taker_trade" or event == "maker_trade"):
            if (not 'client_order_id' in payload):
                logging.warning(
                    "No 'client_order_id' in TickSpread %s payload", event)
                return
            if (not 'execution_amount' in payload):
                logging.warning(
                    "No 'execution_amount' in TickSpread %s payloadk", event)
                return
            clordid = int(payload['client_order_id'])
            execution_amount = int(payload['execution_amount'])
            print("clordid = %d, execution_amount = %d" %
                  (clordid, execution_amount))
            self.receive_exec_trade(event, clordid, execution_amount)
        elif (event == "trade"):
            pass
        elif (event == "balance"):
            pass
        elif (event == "phx_reply"):
            pass
        elif (event == "partial"):
            pass
        elif (event == 'update_position'):
            pass
        else:
            print("UNKNOWN EVENT: %s" % event)
            print(data)

    def ftx_callback(self, data):
        new_price = None
        if ("p" in data):
            new_price = float(data["p"])

        if ("data" in data):
            for trade_line in data["data"]:
                if ("price" in trade_line):
                    new_price = trade_line["price"]

        if (new_price != None):
            self.active = True
            self.fair_price = new_price
            self.spread = 0.00002
            self.update_orders()

    def callback(self, source, raw_data):
        logging.info("<-%-10s: %s", source, raw_data)

        data = json.loads(raw_data)
        if (source == 'tickspread'):
            self.tickspread_callback(data)
        elif (source == 'ftx'):
            self.ftx_callback(data)


async def main():
    api = TickSpreadAPI()
    api.register("maker@tickspread.com", "maker")
    time.sleep(3.0)
    login_status = api.login("maker@tickspread.com", "maker")
    if (not login_status):
        asyncio.get_event_loop().stop()
        return 1

    mmaker = MarketMaker(api)

    #bybit_api = ByBitAPI()
    ftx_api = FTXAPI()
    #binance_api = BinanceAPI()
    #bitmex_api = BitMEXAPI()
    #huobi_api = HuobiAPI()

    await api.connect()
    await api.subscribe("market_data", {"symbol": "BTC-PERP"})
    await api.subscribe("user_data", {"symbol": "BTC-PERP"})
    api.on_message(mmaker.callback)

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

    #await bitmex_api.connect()
    #bitmex_api.on_message(mmaker.callback)

    #await huobi_api.connect()
    #await huobi_api.subscribe()
    #huobi_api.on_message(mmaker.callback)


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()
