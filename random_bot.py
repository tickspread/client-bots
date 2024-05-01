
# -*- coding: utf-8 -*-
"""Example TickSpread Bot

This module gives an example market-making bot that listens to market-data
feeds from external exchanges (Binance, Huobi, BitMEX and Bybit) and
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
import os
import argparse
import logging.handlers

from decimal import Decimal
from tickspread_api import TickSpreadAPI
# from python_loopring.tickspread_dex import TickSpreadDex
from outside_api import ByBitAPI, BinanceAPI, BitMEXAPI, HuobiAPI, PythXauAPI

import statistics
from datetime import datetime
from pyblake2 import blake2b
import hashlib

parser = argparse.ArgumentParser(
    description='Run a market maker bot on TickSpread exchange.')
parser.add_argument('--id', dest='id', default="0",
                    help='set the id to run the account (default: 0)')
parser.add_argument('--env', dest='env', default="prod",
                    help='set the env to run the account (default: prod)')
parser.add_argument('--log', dest='log', default="shell",
                    help='set the output for the logs (default: shell)')
parser.add_argument('--dex', dest='dex', default="false",
                    help='set the tyoe of exchange (default: prod)')
parser.add_argument('--tickspread_password', dest='tickspread_password', default="maker",
                    help='set the tickspread_password to login (default: maker)')
parser.add_argument('--market', dest='market', required=True)
parser.add_argument('--external_market', dest='external_market', required=True)
parser.add_argument('--money_asset', dest='money_asset', required=True)

args = parser.parse_args()
id = args.id
env = args.env
log_file = args.log
dex = True if args.dex == "true" else False
tickspread_password = args.tickspread_password

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s')
if log_file != "shell":
    # log_handler = logging.handlers.WatchedFileHandler(
    #     '/home/ubuntu/store/logs/bot_%s.log' % id)
    log_handler = logging.handlers.WatchedFileHandler(log_file)
    logger = logging.getLogger()
    logger.removeHandler(logger.handlers[0])
    logger.addHandler(log_handler)


class Side(Enum):
    BID = 1
    ASK = 2


def str_to_side(side):
    if side == "bid":
        return Side.BID
    elif side == "ask":
        return Side.ASK
    else:
        return 0


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


def order_cancel_to_str(cancel):
    if (cancel == CancelState.NORMAL):
        return " "
    elif (cancel == CancelState.PENDING):
        return "x"
    else:
        return "?"


MAX_CANCEL_RETRIES = 50


class Order:
    def __init__(self, side, logger):
        self.clordid = None
        self.side = side
        self.price = None
        self.total_amount = 0
        self.amount_left = 0
        self.state = OrderState.EMPTY
        self.cancel = CancelState.NORMAL
        self.cancel_retries = 0
        self.auction_id_send = 0
        self.auction_id_cancel = 0
        self.last_send_time = 0.0
        self.logger = logger

    def __str__(self):
        if (self.state == OrderState.EMPTY):
            return ""
        else:
            return "%s %s/%s @ %s (%s) [%s]%s" % (
                side_to_str(self.side), self.amount_left, self.total_amount,
                str(self.price), str(self.clordid), order_state_to_str(self.state), order_cancel_to_str(self.cancel))


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

        self.last_status_time = 0.0

        self.top_order = 0
        self.top_price = None
        self.orders = []
        for i in range(self.max_orders):
            self.orders.append(Order(self.side, self.parent.logger))

    def debug_orders(self):
        for i in range(self.max_orders):
            index = (self.top_order + i) % (self.max_orders)
            # if (self.orders[index].state != OrderState.EMPTY):
            self.parent.logger.info("%d: %s", index, self.orders[index])

    def set_new_price(self, new_price):
        if (self.side == Side.BID):
            new_top_price = Decimal(math.floor(
                new_price / self.tick_jump) * self.tick_jump)
            #print("BID", new_price, new_top_price)
        else:
            new_top_price = Decimal(math.ceil(
                new_price / self.tick_jump) * self.tick_jump)
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
        self.parent.logger.info(
            "___DEBUG NEW ORDER__ %s - top: %d => %d" %
            (side_to_str(self.side), self.old_top_order, self.top_order))
        self.parent.logger.info(
            "___DEBUG NEW PRICE__ of %s %s - top: %s => %s" %
            (new_price, side_to_str(self.side), self.old_top_price, self.top_price))

    def maybe_cancel_top_orders(self):
        self.parent.logger.info("maybe_cancel_top_orders: %d" % self.top_price)
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
        current_time = time.time()
        self.parent.logger.info("recalculate_top_orders: %d, top = (%d => %d) %s %d [time = %f/%f, available = %.2f]", self.top_price,
                                self.old_top_order, self.top_order, side_to_str(self.side), self.available_limit, current_time, self.last_status_time+1.0, self.available_limit)

        if (current_time - self.last_status_time > 1.0):
            self.debug_orders()
            self.last_status_time = current_time

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
                #self.parent.logger.info("breaking at %d", self.top_order + i)
                # Send new orders at the bottom later
                break
            index = (self.top_order + i) % (self.max_orders)
            order = self.orders[index]
            if (order.state == OrderState.EMPTY):
                size = min(self.order_size, self.available_limit)
                self.parent.logger.info(
                    "Found empty order %d, will send NEW with size %d", index, size)
                if (size > 0):
                    self.parent.send_new(order, size, price)
            if (order.state != OrderState.EMPTY
                    and order.cancel == CancelState.NORMAL):
                if (price != order.price):
                    self.parent.send_cancel(order)
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
                        self.parent.send_new(order, size, price)

                if (order.state != OrderState.EMPTY
                        and order.cancel == CancelState.NORMAL):
                    if (price != order.price):
                        self.parent.send_cancel(order)

                price += price_increment

    def maybe_cancel_bottom_orders(self):
        self.parent.logger.info("maybe_cancel_bottom_orders: %d",
                                self.top_price)
        # self.debug_orders()

        for i in range(self.target_num_orders, self.max_orders):
            index = (self.top_order + i) % (self.max_orders)
            order = self.orders[index]

            if (order.state != OrderState.EMPTY
                    and order.cancel == CancelState.NORMAL):
                self.parent.send_cancel(order)


class MarketMaker:
    def __init__(self, api, *, logger=logging.getLogger(),
                 name="bot_example", version="0.0",
                 orders_per_side=8, max_position=400, tick_jump=10, order_size=0.5, leverage=10,
                 initial_price="155.8", volatility="0.001"):
        # System
        self.api = api
        self.logger = logger
        self.name = name
        self.version = version

        # Structure
        self.bids = MarketMakerSide(self, side=Side.BID,
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    order_size=order_size, available_limit=max_position, tick_jump=tick_jump)
        self.asks = MarketMakerSide(self, side=Side.ASK,
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    order_size=order_size, available_limit=max_position, tick_jump=tick_jump)

        # Market State
        self.last_auction_id = 0
        
        self.has_execution_band = False
        self.execution_band_low = None
        self.execution_band_high = None

        # User State
        self.has_user_balance = False
        self.balance_available = 0
        self.balance_frozen = 0

        self.has_old_orders = False
        self.old_orders = []

        self.has_user_position = False
        self.position = 0
        self.position_entry_price = 0
        self.position_liquidation_price = 0
        self.position_total_margin = 0
        self.position_funding = 0
        
        if dex == True:
            self.has_user_balance = True
            self.has_old_orders = True
            self.has_user_position = True

        # PnL
        self.gross_profit = 0
        self.fees_paid = 0

        # Parameters
        self.tick_jump = tick_jump
        self.order_size = order_size
        self.leverage = leverage
        self.symbol = args.market
        self.money = args.money_asset
        self.max_position = max_position

        # State
        self.real = True
        self.active = False
        self.fair_price = None
        self.spread = None
        
        # Random markets
        # Market configuration
        self.initial_price = Decimal(initial_price)
        self.volatility = Decimal(volatility)
        self.seed = None

        # Current state variables
        self.current_base_price = Decimal(initial_price)
        self.current_tick_counter = 0

        # Next state variable
        self.next_base_price = Decimal(initial_price)
        self.next_tick_counter = 0
        
        # Set the file name based on the current date
        self.state_filename = f"market_maker_state_{datetime.utcnow().strftime('%Y_%m_%d')}.json"
        
        self.initialized = asyncio.Event()  # Event to signal initialization completion
        self.state_loaded = False

    def log_new(self, side, amount, price, clordid):
        self.logger.info("->NEW %s %s @ %s (%d)" %
                         (side_to_str(side), amount, price, clordid))

    def log_cancel(self, side, amount_left, price, clordid):
        self.logger.info("->CAN %s %s @ %s (%d)" %
                         (side_to_str(side), amount_left, price, clordid))

    def send_new(self, order, amount, price):
        # if (order.side == Side.BID):
        #     self.bid_available_limit -= amount
        # else:
        #     assert(order.side == SIDE.ASK)
        #     self.ask_available_limit -= amount
        
        clordid = self.api.get_next_clordid()

        self.log_new(order.side, amount, price, clordid)

        order.last_send_time = time.time()
        if (self.real):
            self.register_new(order, clordid, amount, price)
            self.api.create_order(amount=amount,
                                  price=price,
                                  leverage=self.leverage,
                                  symbol=self.symbol,
                                  side=side_to_str(order.side),
                                  asynchronous=True,
                                  batch=True)

    def send_cancel(self, order):
        self.log_cancel(order.side, order.amount_left,
                        order.price, order.clordid)

        if (self.real):
            self.register_cancel(order)
            self.api.delete_order(order.clordid, symbol=self.symbol, asynchronous=True, batch=True)
        
        if dex:
            self._delete_order(order)

    def register_new(self, order, clordid, amount, price):
        assert (order.state == OrderState.EMPTY)
        order.state = OrderState.PENDING
        order.cancel = CancelState.NORMAL
        order.clordid = clordid
        order.total_amount = amount
        order.amount_left = amount
        order.price = price
        order.auction_id_send = self.last_auction_id

    def register_cancel(self, order):
        assert (order.cancel == CancelState.NORMAL)
        order.cancel = CancelState.PENDING
        order.auction_id_cancel = self.last_auction_id

    def _delete_order(self, order):
        if (order.side == Side.BID):
            self.bids.available_limit += order.amount_left
        else:
            self.asks.available_limit += order.amount_left

        order.state = OrderState.EMPTY
        order.cancel = CancelState.NORMAL
        order.cancel_retries = 0
        order.total_amount = 0
        order.amount_left = 0
        order.clordid = None
        order.price = None

    def exec_ack(self, order):
        if (order.state != OrderState.PENDING):
            self.logger.warning(
                "Received acknowledge, but order %d is in state %s",
                order.clordid, order_state_to_str(order.state))
        order.state = OrderState.ACKED
        self.logger.info("Sent: %d, Received: %d",
                         order.auction_id_send, self.last_auction_id)

    def exec_reject(self, order):
        if (order.state != OrderState.PENDING):
            self.logger.warning(
                "Received reject, but order %d is in state %s",
                order.clordid, order_state_to_str(order.state))
        self._delete_order(order)

    def exec_cancel_reject(self, order):
        self.logger.info("Received reject_cancel in order %d", order.clordid)
        if (order.state != OrderState.PENDING):
            self.logger.info(
                "Received reject_cancel for order %d in state %s",
                order.clordid, order_state_to_str(order.state))
        if (order.cancel == CancelState.NORMAL):
            self.logger.warning(
                "Received reject_cancel, but order %d was not waiting for cancel",
                order.clordid)
        order.cancel_retries += 1

        if (order.cancel_retries >= MAX_CANCEL_RETRIES):
            if (order.state == OrderState.PENDING):
                self.logger.warning(
                    "Order %d has been cancelled %d times, still pending, assume was never sent")
                self._delete_order(order)
            else:
                self.logger.error(
                    "Order %d has been cancelled more than %d times, giving up", order.clordid, MAX_CANCEL_RETRIES)
                # logging.shutdown()
                # sys.exit(1)
        else:
            order.cancel = CancelState.NORMAL
            # TODO send another cancel immediately

    def exec_maker(self, order):
        if (order.state != OrderState.ACKED and
                order.state != OrderState.ACTIVE):
            self.logger.warning(
                "Received maker_order, but order %d is in state %s",
                order.clordid, order_state_to_str(order.state))
        order.state = OrderState.MAKER

    def exec_active(self, order):
        if (order.state != OrderState.ACKED and
                order.state != OrderState.MAKER):
            self.logger.warning(
                "Received active_order, but order %d is in state %d",
                order.clordid, order_state_to_str(order.state))
        if (order.state == OrderState.ACKED):
            self.logger.warning(
                "Received active_order in order %d at state %d, are we pushing execution_bands?",
                order.clordid, order_state_to_str(order.state))
        order.state = OrderState.ACTIVE

    def exec_remove(self, order):
        if (order.cancel != CancelState.PENDING):
            self.logger.warning("Received unexpected remove_order in id %d",
                                order.clordid)
        self._delete_order(order)

        self.logger.info("Canc: %d, Received: %d",
                         order.auction_id_cancel, self.last_auction_id)

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
            # logging.warning("Received exec %s for unknown order: %d",
            #                 event, clordid)
            return

        self.logger.info("Received exec for order %d", order.clordid)

        if (event == "acknowledge_order"):
            self.exec_ack(order)
        elif (event == "maker_order"):
            self.exec_maker(order)
        elif (event == "delete_order"):
            self.exec_remove(order)
        elif (event == "abort_create"):
            self.exec_remove(order)
        elif (event == "active_order"):
            self.exec_active(order)
        elif (event == "reject_order"):
            self.exec_reject(order)
        elif (event == "reject_cancel"):
            self.exec_cancel_reject(order)
        else:
            self.logger.warning("Order %d received unknown event %s",
                                order.clordid, event)

    def _trade(self, order, execution_amount):
        if (execution_amount > order.amount_left):
            self.logger.error(
                "Received trade with execution_amount %d in order %d, but order has amount_left = %d"
                % (execution_amount, order.clordid, order.amount_left))
            logging.shutdown()
            sys.exit(1)
        # Update Order
        order.amount_left -= execution_amount
        if (order.amount_left == 0):
            order.state = OrderState.EMPTY
            order.cancel = CancelState.NORMAL
            order.cancel_retries = 0
            order.total_amount = 0
            order.clordid = None
            order.price = None

    def receive_exec_trade(self, event, clordid, execution_amount, side):
        if (clordid):
            order = self.find_order_by_clordid(clordid)
            if (not order):
                logging.warning("Received exec trade %s for unknown order: %d",
                                event, clordid)
            else:
                if (event == "maker_trade"):
                    if (order.state != OrderState.MAKER and
                            order.state != OrderState.ACTIVE):
                        self.logger.warning(
                            "Received maker_trade, but order %d is in state %s",
                            order.clordid, order_state_to_str(order.state))
                    self._trade(order, execution_amount)
                elif (event == "taker_trade"):
                    if (order.state != OrderState.ACKED and
                            order.state != OrderState.ACTIVE):
                        self.logger.warning(
                            "Received taker_trade, but order %d is in state %s",
                            order.clordid, order_state_to_str(order.state))
                    self._trade(order, execution_amount)
                else:
                    logging.warning("Order %d received unknown trade event %s",
                                    order.clordid, event)
        else:
            pass

        # Update Position
        if (side == "bid"):
            self.position += execution_amount
            self.asks.available_limit += execution_amount
        else:
            assert(side == "ask")
            self.position -= execution_amount
            self.bids.available_limit += execution_amount

    def update_orders(self):
        self.logger.info("update_orders")
        assert (self.active)
        self.bids.set_new_price(min(self.fair_price - self.spread, self.execution_band_high))
        self.logger.info("SETTING NEW ASK PRICE. FAIR+SPREAD %s EXECUTION BAND LOW %s", self.fair_price + self.spread, self.execution_band_low)
        self.asks.set_new_price(max(self.fair_price + self.spread, self.execution_band_low))
        
        

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
        
        self.api.dispatch_batch()
    
    def tickspread_market_data_partial(self, payload):
        print("MARKET DATA PARTIAL: ", payload)
        if (not 'execution_band' in payload or payload['execution_band'] == None):
            logging.warning("No execution_band in market_data partial")
            return
            
        execution_band = payload['execution_band']
        
        if (not 'high' in execution_band):
            logging.warning("No high in execution_band")
            return
        
        if (not 'low' in execution_band):
            logging.warning("No low in execution_band")
            return
            
        
        print(execution_band)
        time.sleep(5.0)
        
        self.execution_band_high = Decimal(execution_band['high'])
        self.execution_band_low = Decimal(execution_band['low'])
        self.has_execution_band = True
        
        if self.has_execution_band:
            if self.has_user_balance and self.has_old_orders and self.has_user_position:
                self.initialized.set()  # Ensure all are still true and set

    def tickspread_user_data_partial(self, payload):
        print("USER DATA PARTIAL")
        if (not 'balance' in payload):
            logging.warning("No balance in user_data partial")
            return

        if (not 'orders' in payload):
            logging.warning("No orders in user_data partial")
            return

        if (not 'positions' in payload):
            logging.warning("No positions in user_data partial")
            return

        print(payload)
        balance = payload['balance']
        orders = payload['orders']
        positions = payload['positions']

        found_money_balance = False
        for each_balance in balance:
            if (not 'asset' in each_balance or
                not 'available' in each_balance or
                    not 'frozen' in each_balance):
                logging.warning(
                    "Missing at least one of ['asset', 'available', 'frozen'] in balance element")
                continue

            asset = each_balance['asset']
            available = Decimal(each_balance['available'])
            frozen = Decimal(each_balance['frozen'])

            if (asset == self.money):
                self.balance_available = available
                self.balance_frozen = frozen
                found_money_balance = True

        if (not found_money_balance):
            logging.warning(
                "Could not find %s balance in partial" % self.money)
            return
        self.has_user_balance = True

        self.old_orders = orders
        self.has_old_orders = True

        if (self.old_orders):
            print("Found %d old orders" % len(self.old_orders))

        found_symbol_position = False
        for position in positions:
            if (not 'market' in position or
                not 'amount' in position or
                not 'funding' in position or
                not 'entry_price' in position or
                not 'liquidation_price' in position or
                not 'total_margin' in position):
                logging.warning(
                    "Missing at least one of ['amount', 'funding', 'entry_price', 'liquidation_price', 'total_margin'] in position element")
                continue
            symbol = position['market']
            amount = Decimal(position['amount'])
            funding = Decimal(position['funding'])
            total_margin = Decimal(position['total_margin'])
            entry_price = Decimal(position['entry_price'])
            liquidation_price = Decimal(position['liquidation_price'])
            
            #total_price = position['total_price']

            if (symbol == self.symbol):
                assert(self.position == 0)
                self.position = amount
                self.bids.available_limit -= amount
                self.asks.available_limit += amount
                
                self.position_entry_price = entry_price
                self.position_liquidation_price = liquidation_price
                self.position_total_margin = total_margin
                self.position_funding = funding
                found_symbol_position = True


        if (not found_symbol_position):
            logging.warning(
                "Could not find %s position in partial" % self.symbol)
            self.position = 0
        self.has_user_position = True
        print("Read user_data partial successfully")
        
        if self.has_user_balance and self.has_old_orders and self.has_user_position and self.has_execution_band:
            self.initialized.set()  # Signal that initialization is complete

    def cancel_old_orders(self):
        print("Old orders:")
        for old_order in self.old_orders:
            print(old_order)
            if (not 'client_order_id' in old_order or
                not 'amount' in old_order or
                not 'price' in old_order or
                not 'side' in old_order or
                not 'market' in old_order):
                logging.warning(
                    "Could not find one of ['client_order_id', 'amount', 'side', 'market'] in order from partial")
                return
            client_order_id = old_order['client_order_id']
            amount = old_order['amount']
            price = old_order['price']
            side = old_order['side']

            print("To Log Cancel")
            # FIXME use left instead of amount
            self.log_cancel(str_to_side(side), amount, price, client_order_id)

            print("After Log Cancel")
            self.api.delete_order(
                old_order['client_order_id'], symbol=old_order['market'], asynchronous=False, batch=True)
        self.api.dispatch_batch()
        return

    def tickspread_callback(self, data):
        if (not 'event' in data):
            logging.warning("No 'event' in TickSpread message")
            return 1
        if (not 'payload' in data):
            logging.warning("No 'payload' in TickSpread message")
            return 1
        if (not 'topic' in data):
            logging.warning("No 'topic' in TickSpread message")
            return 1

        event = data['event']
        payload = data['payload']
        topic = data['topic']
        
        if (event == "partial"):
            if (topic == "user_data"):
                self.tickspread_user_data_partial(payload)
                #print("OK_1")                    
                self.cancel_old_orders()
                #print("FINISH_OK")
                return 0
            
            if (topic == "market_data"):
                self.tickspread_market_data_partial(payload)
        
        if (event == "update"):
            if (not 'auction_id' in payload):
                self.logger.warning(
                    "No 'auction_id' in TickSpread %s payload", event)
                return 0

            auction_id = int(payload['auction_id'])
            if (self.last_auction_id and auction_id != self.last_auction_id + 1):
                self.logger.warning(
                    "Received auction_id = %d, last was %d", auction_id, self.last_auction_id)
                return 0
            self.last_auction_id = auction_id
            #self.logger.info("AUCTION: %d" % auction_id)
            
            if ('execution_band' in payload):
                execution_band = payload['execution_band']
                if (not 'high' in execution_band):
                    self.logger.warning("No high in execution_band")
                    return 0
                if (not 'low' in execution_band):
                    self.logger.warning("No low in execution_band")
                    return 0
                self.execution_band_high = Decimal(execution_band['high'])
                self.execution_band_low = Decimal(execution_band['low'])
                self.update_orders()
        elif (event == "acknowledge_order" or event == "maker_order"
              or event == "delete_order" or event == "abort_create"
              or event == "active_order" or event == "reject_order"
              or event == "reject_cancel"):
            #print("receive accept: ", event)
            if (not 'client_order_id' in payload):
                self.logger.warning(
                    "No 'client_order_id' in TickSpread %s payload", event)
                return 0
            clordid = int(payload['client_order_id'])
            #print("clordid = %d" % clordid)
            self.receive_exec(event, clordid)
            self.update_orders()
            #print("fin accept")
        elif (event == "taker_trade" or event == "maker_trade" or
              event == "liquidation" or event == "auto_deleverage"):
            #print("receive trade")
            if (not 'client_order_id' in payload):
                self.logger.warning(
                    "No 'client_order_id' in TickSpread %s payload", event)
                clordid = 0
            else:
                clordid = int(payload['client_order_id'])

            if (not 'execution_amount' in payload):
                self.logger.warning(
                    "No 'execution_amount' in TickSpread %s payload", event)
                return 0
            if (not 'side' in payload):
                self.logger.warning(
                    "No 'side' in TickSpread %s payload", event)
                return 0
            execution_amount = Decimal(payload['execution_amount'])
            #print("clordid = %d, execution_amount = %d" % (clordid, execution_amount))
            self.receive_exec_trade(
                event, clordid, execution_amount, payload['side'])
            self.update_orders()
        elif (event == "trade"):
            pass
        elif (event == "balance"):
            pass
        elif (event == "phx_reply"):
            pass
        elif (event == "partial"):
            pass
        elif (event == "update_position"):
            pass
        elif (event == "reject_cancel"):
            pass
        else:
            print("UNKNOWN EVENT: %s" % event)
            print(data)
        return 0
    
    async def validate_orders(self):
        if not self.active:
            print("Market maker is not active, cannot validate orders.")
            return False
        
        non_empty_orders = [order for order in self.bids.orders + self.asks.orders if order.state != OrderState.EMPTY]
        maker_ready = all(order.state == OrderState.MAKER for order in non_empty_orders)
        pending_cancellations = [order for order in non_empty_orders if order.cancel == CancelState.PENDING]
        if pending_cancellations:
            #self.logger.error("Pending cancellations did not clear after timeout.")
            for order in pending_cancellations:
                self.logger.info(f"Still pending: Order ID {order.clordid}")
            return False
        if not maker_ready:
            non_maker_orders = [order for order in non_empty_orders if order.state != OrderState.MAKER]
            non_maker_orders_str = ', '.join(str(order) for order in non_maker_orders)
            #self.logger.info("Timeout reached: Not all non-empty orders transitioned to MAKER state.")
            self.logger.info(f'NON MAKER ORDERS: {non_maker_orders_str}')
            return False
        # timeout = 20
        # check_interval = 0.5
        # elapsed_time = 0
        # while elapsed_time < timeout:
        #     non_empty_orders = [order for order in self.bids.orders + self.asks.orders if order.state != OrderState.EMPTY]
        #     maker_ready = all(order.state == OrderState.MAKER for order in non_empty_orders)
        #     pending_cancellations = [order for order in non_empty_orders if order.cancel == CancelState.PENDING]

        #     if maker_ready and not pending_cancellations:
        #         break
            
        #     for order in non_empty_orders:
        #         self.logger.info(f"Checking order ID {order.clordid}, State {order.state}, Cancel State {order.cancel}")
            
        #     await asyncio.sleep(check_interval)
        #     elapsed_time += check_interval
                
        # if elapsed_time >= timeout:
        #     if pending_cancellations:
        #         self.logger.error("Pending cancellations did not clear after timeout.")
        #         for order in pending_cancellations:
        #             self.logger.error(f"Still pending: Order ID {order.clordid}")
        #         return False
        #     if not maker_ready:
        #         non_maker_orders = [order for order in non_empty_orders if order.state != OrderState.MAKER]
        #         non_maker_orders_str = ', '.join(str(order) for order in non_maker_orders)
        #         self.logger.error("Timeout reached: Not all non-empty orders transitioned to MAKER state.")
        #         self.logger.error(f'NON MAKER ORDERS: {non_maker_orders_str}')
        #         return False

        maker_bids = [order for order in self.bids.orders if order.state == OrderState.MAKER]
        maker_asks = [order for order in self.asks.orders if order.state == OrderState.MAKER]
        
        self.asks.debug_orders()
        self.bids.debug_orders()

        top_bid_index = self.bids.top_order % self.bids.max_orders
        top_ask_index = self.asks.top_order % self.asks.max_orders

        top_bid = self.bids.orders[top_bid_index]
        top_ask = self.asks.orders[top_ask_index]

        # Check proximity to fair price within tick_jump and spread
        bid_price_limit = self.fair_price - (self.spread + self.tick_jump)
        if not (bid_price_limit <= top_bid.price <= self.fair_price):
            self.logger.error(f"Top bid order price {top_bid.price} is not within the expected range from the fair price {self.fair_price}, adjusted by spread and tick jump downwards to {bid_price_limit}.")
            return False

        ask_price_limit = self.fair_price + (self.spread + self.tick_jump)
        if not (self.fair_price <= top_ask.price <= ask_price_limit):
            self.logger.error(f"Top ask order price {top_ask.price} is not within the expected range from the fair price {self.fair_price}, adjusted by spread and tick jump upwards to {ask_price_limit}.")
            return False
                
        if len(maker_bids) != self.bids.target_num_orders or len(maker_asks) != self.asks.target_num_orders:
            self.logger.error(f"Incorrect number of maker orders on either side. BIDS: {self.bids.target_num_orders} ASKS: {self.asks.target_num_orders}")
            return False
        
        return True
        
    def save_state(self):
        try:
            state = {
                'seed': self.seed,
                'volatility': str(self.volatility),
                'tick_counter': self.next_tick_counter,
                'last_price': str(self.next_base_price),
                'base_price': str(self.initial_price)
            }
            with open(self.state_filename, 'w') as f:
                json.dump(state, f)
            # Since it was saved, update current states
            self.current_tick_counter = self.next_tick_counter
            self.current_base_price = self.next_base_price
            self.logger.info(f"State saved successfully of price {self.current_base_price} and tick {self.current_tick_counter}")
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
            raise Exception(f"Failed to save state: {e}") 
    
    def generate_price(self):
        print(f"GENERATING NEW PRICE BASED ON {self.current_base_price} AND SEED {self.seed} AND TICK {self.current_tick_counter} +1")
        tick = self.current_tick_counter + 1
        combined_value = bytes(self.seed + str(tick), 'ascii')
        hash_value = blake2b(combined_value, digest_size=32).digest()
        unsigned_int = int.from_bytes(hash_value[:8], 'little')
        unit_range = float(unsigned_int) / (2**64 - 1)
        normal_return = statistics.NormalDist(mu=0.0, sigma=float(self.volatility)).inv_cdf(unit_range)

        new_price = self.current_base_price * (1 + Decimal(normal_return))
        self.next_base_price = new_price  # Update the base price for the next generation
        self.next_tick_counter = tick   
        print(f"NEW PRICE {new_price}")

        return {"p": new_price}

    def load_state(self):
        try:
            if os.path.exists(self.state_filename):
                with open(self.state_filename, 'r') as file:
                    state = json.load(file)
                    seed = state['seed']
                    volatility = Decimal(state['volatility'])
                    current_tick_counter = int(state['tick_counter'])
                    initial_price = Decimal(state['base_price'])
                    current_base_price = Decimal(state['last_price'])

                # Replay from initial price to verify current base price
                price = initial_price
                print(f"Beginning Price = {price} for Seed {seed} and tick {current_tick_counter}")
                for tick in range(current_tick_counter + 1):
                    combined_value = bytes(seed + str(tick), 'ascii')
                    hash_value = blake2b(combined_value, digest_size=32).digest()
                    unsigned_int = int.from_bytes(hash_value[:8], 'little')
                    unit_range = float(unsigned_int) / (2**64 - 1)
                    normal_return = statistics.NormalDist(mu=0.0, sigma=float(volatility)).inv_cdf(unit_range)
                    price *= (1 + Decimal(normal_return))
                    self.logger.info(f"Tick {tick}: Price = {price}")
                    
                self.logger.info(f"Expected base price: {current_base_price}, Calculated base price: {price}")

                if price != current_base_price:
                    raise ValueError("Replay failed: recalculated price does not match the saved current base price")

                # Prepare next state parameters based on current state
                self.seed = seed
                self.volatility = volatility
                self.initial_price = initial_price
                self.current_tick_counter = current_tick_counter
                self.current_base_price = current_base_price
                
                self.state_loaded = True
                print("State loaded and replay verified successfully.")
            else:
                self.seed = hashlib.blake2b(datetime.utcnow().strftime('%Y%m%d').encode(), digest_size=32).hexdigest()
                self.next_tick_counter = -1
                self.next_base_price = Decimal(self.initial_price)
                self.initial_price = Decimal(self.initial_price)
                self.volatility = Decimal(self.volatility)
                self.state_loaded = False
                self.logger.info(f"GENERATED SEED {self.seed}")
            
        except Exception as e:
            self.logger.error("Failed to load or verify state through replay: %s", e)
            raise ValueError("Failed to load or verify state through replay") from e

    def common_callback(self, data):
        try:
            # self.logger.info("common_callback")
            
            self.logger.info("callback data:", data)
            new_price = None
            if ("p" in data):
                new_price = Decimal(data["p"])
            if ("data" in data):
                if ("p" in data["data"]):
                    new_price = Decimal(data["data"]["p"])
                else:
                    for trade_line in data["data"]:
                        if ("price" in trade_line):
                            new_price = Decimal(trade_line["price"])
            
            self.logger.info("new_price = %.2f" % new_price)
            if (new_price != None):
                if (not self.active and
                        self.has_user_balance and
                        self.has_old_orders and
                        self.has_user_position and
                        self.has_execution_band):
                    self.active = True
                    self.logger.info("Activating: %d (%d/%d)", self.position,
                                    self.bids.available_limit, self.asks.available_limit)
                elif not self.active:
                    print(self.has_user_balance, self.has_old_orders, self.has_user_position, self.has_execution_band)

                #self.logger.info("active = %s" % str(self.active))
                if (self.active):
                    factor = Decimal(1) - Decimal(0.001) * self.position / self.max_position
                    self.fair_price = new_price * Decimal(factor)
                    #self.spread = Decimal(1.00000001)
                    self.spread = Decimal(0.00010)
                    self.update_orders()

            return 0
        except Exception as e:
            self.logger.error("Error in callback: %s", e)
            sys.exit("Critical error in callback, stopping bot")
            
    def callback(self, source, raw_data):
        #self.logger.info("<-%-10s: %s", source, raw_data)

        if isinstance(raw_data, dict):
            data = raw_data
        else:
            data = json.loads(raw_data)

        rc = 0
        if (source == 'tickspread'):
            rc = self.tickspread_callback(data)
        return rc

async def main():
    if dex == True:
        pass
    else:
        api = TickSpreadAPI(id_multiple=1000, env=env)
        mmaker = MarketMaker(api, tick_jump=Decimal("0.05"), orders_per_side=10,
                             order_size=Decimal("0.01"), max_position=Decimal("50.0"))

        print("REGISTER")
        api.register('maker%s@tickspread.com' % id, tickspread_password)
        time.sleep(0.3)
        print("LOGIN")
        # CHANGE ID MULTIPLE to 100 above when moving back to maker@tickspread.com
        login_status = api.login('maker%s@tickspread.com' %
                                id, tickspread_password)
        if (not login_status):
            asyncio.get_event_loop().stop()
            print("Login Failure")
            return 1
        print("STARTING")

        await api.connect()
        await api.subscribe("market_data", {"symbol": args.market})
        await api.subscribe("user_data", {"symbol": args.market})
        api.on_message(mmaker.callback)
        # Wait for initial data to be fully processed
        await mmaker.initialized.wait()  # Wait until initial setup conditions are confirmed through data handlers
        
        mmaker.load_state()
        
        if mmaker.state_loaded:
            # Use the price loaded from state for the first callback
            price_data = mmaker.generate_price()
            mmaker.common_callback(price_data)
        else:
            # If no state was loaded, start with the initial price generation
            initial_price_data = {"p": mmaker.next_base_price}
            mmaker.common_callback(initial_price_data)

        attempts = 0
        while attempts < 30:
            validated = await mmaker.validate_orders()
            if validated:
                mmaker.save_state()
                price_data = mmaker.generate_price()
                mmaker.common_callback(price_data)
                attempts = 0
            else:
                print("Attempting order validation again...")
                attempts += 1
                if attempts >= 30:
                    print("Failed to validate orders after multiple attempts.")
                    break
            await asyncio.sleep(1)

    print("FINISH INIT")


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.set_debug(False)
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        logging.shutdown()
        exit()
    except SystemExit as e:
        logging.shutdown()
        exit()
