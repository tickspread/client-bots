# -*- coding: utf-8 -*-
"""Quiver Bot (AMM-style)

This bot doesn't look at external markets.
Instead it trades on quiver only, simulating the behavior of an AMM (but for futures)
It checks its own balance & position.
It takes a liquidity parameter, a "neutral_price" (where position is zero).
It also takes a maximum price (so it is intended for futures that do have maximum prices)

Use this bot are your own risk! No guarantees -- it probably has bugs!

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

parser = argparse.ArgumentParser(
    description='Run a market maker bot on TickSpread exchange.')
parser.add_argument('--id', dest='id', default="0",
                    help='set the id to run the account (default: 0)')
parser.add_argument('--env', dest='env', default="prod",
                    help='set the env to run the account (default: prod)')
parser.add_argument('--log', dest='log', default="shell",
                    help='set the output for the logs (default: shell)')
parser.add_argument('--tickspread_password', dest='tickspread_password', default="maker",
                    help='set the tickspread_password to login (default: maker)')
parser.add_argument('--market', dest='market', required=True)
parser.add_argument('--money_asset', dest='money_asset', required=True)

parser.add_argument('--liquidity', dest='liquidity', required=True)
parser.add_argument('--neutral_price', dest='neutral_price', required=True)
parser.add_argument('--max_position', dest='max_position', required=True)
parser.add_argument('--tick_jump', dest='tick_jump', required=True)


args = parser.parse_args()
id = args.id
env = args.env
log_file = args.log
dex = True if args.dex == "true" else False
tickspread_password = args.tickspread_password

logging.basicConfig(level=logging.INFO,
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
        self.parent.logger.debug(
            "%s - top: %d => %d" %
            (side_to_str(self.side), self.old_top_order, self.top_order))

    def maybe_cancel_top_orders(self):
        #self.parent.logger.debug("maybe_cancel_top_orders: %d" % self.top_price)
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
            '''
            self.parent.logger.info("index = %d (%d)",
                                    self.top_order + i,
                                    (self.top_order + i) % self.max_orders)
            '''
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
                 orders_per_side=8, max_position=400, tick_jump=10, order_size=0.5, leverage=10):
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
            logging.warning("Received exec %s for unknown order: %d",
                            event, clordid)
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

        ## Update prices (!)
        new_price = self.calculate_amm_price()
        self.set_new_price(new_price)

    def update_orders(self):
        self.logger.info("update_orders")
        assert (self.active)
        self.bids.set_new_price(min(self.fair_price - self.spread, self.execution_band_high))
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

    def set_new_price(self, new_price):
        if (new_price != None):
            if (not self.active and
                    self.has_user_balance and
                    self.has_old_orders and
                    self.has_user_position and
                    self.has_execution_band):
                self.active = True
                self.logger.info("Activating: %d (%d/%d)", self.position,
                                 self.bids.available_limit, self.asks.available_limit)

            #self.logger.info("active = %s" % str(self.active))
            if (self.active):
                factor = Decimal(1) - Decimal(0.001) * self.position / self.max_position
                self.fair_price = new_price * Decimal(factor)
                self.spread = Decimal(0.00010)
                self.update_orders()
    
    def calculate_amm_price(self):
        price = args.neutral_price - self.position * self.tick_jump / self.order_size
        return price
    
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
            return
        self.has_user_position = True
        print("Read user_data partial successfully")

        new_price = self.calculate_amm_price()
        self.set_new_price(new_price)

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
    api = TickSpreadAPI(id_multiple=1000, env=env)
    # mmaker = MarketMaker(api, tick_jump=Decimal("0.2"), orders_per_side=10,
    #                  order_size=Decimal("1.5"), max_position=Decimal("40.0"))
    
    mmaker = MarketMaker(api, tick_jump=args.tick_jump, orders_per_side=20,
                        order_size=args.liquidity, max_position=args.max_position)
    
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
    
    # These variables are not referred to anywhere, but an object is being created
    # We're passing the mmaker callbacks
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
