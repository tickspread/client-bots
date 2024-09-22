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

from decimal import Decimal, ROUND_DOWN, ROUND_UP, localcontext
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
                 min_order_size, available_limit, tick_jump):
        self.parent = parent
        self.side = side
        self.target_num_orders = target_num_orders
        self.max_orders = max_orders
        self.min_order_size = Decimal(str(min_order_size))
        self.available_limit = Decimal(str(available_limit))
        self.tick_jump = Decimal(str(tick_jump))
        
        self.last_status_time = 0.0

        self.top_order = 0
        self.top_price = None
        self.orders = []
        for i in range(self.max_orders):
            self.orders.append(Order(self.side, self.parent.logger))

    def round_down_to_precision(self, value, precision):
        return value.quantize(precision, rounding=ROUND_DOWN)
    
    def debug_orders(self):
        for i in range(self.max_orders):
            index = (self.top_order + i) % (self.max_orders)
            if (self.orders[index].state != OrderState.EMPTY):
                self.parent.logger.info("%d: %s", index, self.orders[index])

    def set_new_price(self, new_price):
        tick_jump = self.tick_jump
        if self.side == Side.BID:
            new_top_price = (new_price / tick_jump).to_integral_value(rounding=ROUND_DOWN) * tick_jump
        else:
            new_top_price = (new_price / tick_jump).to_integral_value(rounding=ROUND_UP) * tick_jump

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

    def recalculate_all_orders(self):
        current_time = time.time()
        self.parent.logger.info(
            "recalculate_top_orders: %d, top = (%d => %d) %s %d [time = %f/%f, available = %.2f]",
            self.top_price,
            self.old_top_order,
            self.top_order,
            side_to_str(self.side),
            self.available_limit,
            current_time,
            self.last_status_time + 1.0,
            self.available_limit
        )

        if (current_time - self.last_status_time > 1.0):
            self.debug_orders()
            self.last_status_time = current_time

        # Determine price increment direction based on side
        price_increment = self.tick_jump if self.side == Side.ASK else -self.tick_jump
        price = self.top_price

        # Initialize counters
        active_order_count = 0
        total_liquidity = Decimal(0)
        pending_cancel_liquidity = Decimal(0)

        # Loop through the order slots in the circular buffer
        for i in range(self.max_orders):
            index = (self.top_order + i) % self.max_orders
            order = self.orders[index]

            # Calculate delta_ticks: distance from fair price in ticks
            delta_ticks = (price - self.parent.fair_price) / price_increment
            delta_ticks = delta_ticks.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

            # Ensure delta_ticks is non-negative
            if delta_ticks < 0:
                delta_ticks = Decimal('0.000')

            # Calculate expected cumulative liquidity for the current price level
            expected_liquidity = self.parent.avg_tick_liquidity * delta_ticks
            expected_liquidity = min(expected_liquidity, self.parent.max_liquidity)

            # Calculate liquidity deltas based on the liquidity curves
            liquidity_excess = expected_liquidity - (total_liquidity - pending_cancel_liquidity)
            liquidity_needed = expected_liquidity * self.parent.liquidity_curve_hysteresis_low - total_liquidity
            liquidity_min_threshold = (
                expected_liquidity * self.parent.liquidity_curve_hysteresis_minimum
                - total_liquidity
                - self.parent.avg_tick_liquidity
            )

            # Logging for debugging purposes (to be adjusted later)
            self.parent.logger.debug(
                f"Price: {price}, Fair Price: {self.parent.fair_price}, Delta Ticks: {delta_ticks}"
            )
            self.parent.logger.debug(
                f"Expected Liquidity: {expected_liquidity}, "
                f"Liquidity Excess: {liquidity_excess}, "
                f"Liquidity Needed: {liquidity_needed}, "
                f"Liquidity Min Threshold: {liquidity_min_threshold}"
            )

            self.parent.logger.info(str(order))

            # Decide whether to cancel or keep existing orders
            if order.state != OrderState.EMPTY and order.cancel == CancelState.NORMAL:
                if (
                    order.price != price
                    or order.amount_left > liquidity_excess
                    or active_order_count >= self.target_num_orders
                ):
                    # Cancel order due to incorrect price, excess liquidity, or too many orders
                    self.parent.send_cancel(order)
                elif (
                    liquidity_min_threshold > order.amount_left
                    and pending_cancel_liquidity == 0
                    and order.amount_left < self.parent.max_order_size
                ):
                    # Cancel order to replace it with a larger one due to insufficient liquidity
                    self.parent.send_cancel(order)

            # Place new orders if necessary
            if order.state == OrderState.EMPTY and active_order_count < self.target_num_orders:
                if liquidity_needed > self.min_order_size:
                    size = min(liquidity_needed, self.available_limit)
                    size = min(size, self.parent.max_order_size)
                    # Round down the size to the precision of min_order_size
                    rounded_size = self.round_down_to_precision(size, self.min_order_size)
                    self.parent.logger.info(
                        "Found empty order %d, will send NEW with size %s", index, rounded_size
                    )
                    if rounded_size >= self.min_order_size:
                        self.parent.send_new(order, rounded_size, price)

            # Update counters
            if order.state != OrderState.EMPTY:
                active_order_count += 1
                total_liquidity += Decimal(str(order.amount_left))
                if order.cancel == CancelState.PENDING:
                    pending_cancel_liquidity += Decimal(str(order.amount_left))

            # Check exit conditions
            if total_liquidity + self.min_order_size >= self.parent.max_liquidity:
                break

            if active_order_count >= self.target_num_orders:
                break

            # Move to the next price level
            price += price_increment

class MarketMaker:
    def __init__(self, api, *, logger=logging.getLogger(),
                 name="bot_example", version="0.0",
                 orders_per_side=8, max_position=400, tick_jump=10, min_order_size=0.5, leverage=10, max_diff = 0.004, max_liquidity = -1, max_order_size=10.0):
        # System
        self.api = api
        self.logger = logger
        self.name = name
        self.version = version

        # Structure
        self.bids = MarketMakerSide(self, side=Side.BID,
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    min_order_size=min_order_size, available_limit=max_position, tick_jump=tick_jump)
        self.asks = MarketMakerSide(self, side=Side.ASK,
                                    target_num_orders=orders_per_side, max_orders=2*orders_per_side,
                                    min_order_size=min_order_size, available_limit=max_position, tick_jump=tick_jump)

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
        self.tick_jump = Decimal(tick_jump)
        self.min_order_size = Decimal(min_order_size)
        self.max_order_size = Decimal(max_order_size)
        self.leverage = leverage
        self.symbol = args.market
        self.money = args.money_asset
        self.max_diff = Decimal(str(max_diff))
        self.max_position = Decimal(str(max_position))
        if (max_liquidity >= 0):
            self.max_liquidity = Decimal(str(max_liquidity))
        else:
            self.max_liquidity = max_position
        
        # Hardcoded parameters -- the lower the higher the hysteresis
        self.liquidity_curve_hysteresis_low = Decimal(0.9)
        self.liquidity_curve_hysteresis_minimum = Decimal(0.8)

        # State
        self.real = True
        self.active = False
        self.fair_price = None
        self.kyle_impact = None
        self.avg_tick_liquidity = None
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

    def update_orders(self):
        self.logger.info("update_orders")
        assert (self.active)
        self.bids.set_new_price(min(self.fair_price - self.spread, self.execution_band_high))
        self.asks.set_new_price(max(self.fair_price + self.spread, self.execution_band_low))

        self.bids.recalculate_all_orders()
        self.asks.recalculate_all_orders()
    
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
            self.position = 0
        self.has_user_position = True
        print("Read user_data partial successfully")

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

    def common_callback(self, data):
        # self.logger.info("common_callback")
        
        self.logger.info("callback data: %s" % str(data))
        
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
                factor = Decimal(1) - Decimal(self.max_diff) * self.position / self.max_position
                self.fair_price = new_price * Decimal(factor)
                self.kyle_impact = new_price * self.max_diff / self.max_position	# Price Impact (per position unit)
                self.avg_tick_liquidity = self.tick_jump / self.kyle_impact     # On average how much liquidity we want per tick (based on tick jump)
                self.spread = Decimal(0.0)
                self.update_orders()

        self.api.dispatch_batch()
        return 0

    def ftx_callback(self, data):
        return self.common_callback(data)

    def binance_s_callback(self, data):
        return self.common_callback(data)

    def pyth_xau_callback(self, data):
        return self.common_callback(data)

    def callback(self, source, raw_data):
        #self.logger.info("<-%-10s: %s", source, raw_data)

        if isinstance(raw_data, dict):
            data = raw_data
        else:
            data = json.loads(raw_data)

        rc = 0
        if (source == 'tickspread'):
            rc = self.tickspread_callback(data)
        elif (source == 'ftx'):
            rc = self.ftx_callback(data)
        elif (source == 'binance-s'):
            rc = self.binance_s_callback(data)
        elif (source == 'pyth'):
            rc = self.pyth_xau_callback(data)
        return rc


async def main():
    if dex == True:
        # api = TickSpreadDex(id_multiple=1000, env=env)
        # mmaker = MarketMaker(api, tick_jump=0.5, orders_per_side=50,
        #                  min_order_size=0.01, max_position=4000)
        pass
    else:
        api = TickSpreadAPI(id_multiple=1000, env=env)
        # mmaker = MarketMaker(api, tick_jump=Decimal("0.2"), orders_per_side=10,
        #                  min_order_size=Decimal("1.5"), max_position=Decimal("40.0"))

        # if args.market == "XAU-TEST":
        #     mmaker = MarketMaker(api, tick_jump=Decimal("0.01"), orders_per_side=10,
        #                     min_order_size=Decimal("0.20"), max_position=Decimal("50.0"))
            
        # if args.market == "XAU":
        #     mmaker = MarketMaker(api, tick_jump=Decimal("0.01"), orders_per_side=50,
        #                     min_order_size=Decimal("0.05"), max_position=Decimal("5.0"))

        if args.market == "ETH":
            mmaker = MarketMaker(api, tick_jump=Decimal("0.5"), orders_per_side=35,
                            min_order_size=Decimal("0.5"), max_position=Decimal("100.0"))

        if args.market == "SOL":
            mmaker = MarketMaker(api, tick_jump=Decimal("0.05"), orders_per_side=35,
                            min_order_size=Decimal("10.0"), max_position=Decimal("500.0"),
                            max_order_size=Decimal("100.0"))

        if args.market == "BNB":
            mmaker = MarketMaker(api, tick_jump=Decimal("0.5"), orders_per_side=20,
                            min_order_size=Decimal("1.0"), max_position=Decimal("150.0"), max_liquidity=Decimal("70.0"))
        
        # if args.market == "ETH-TEST":
        #     # mmaker = MarketMaker(api, tick_jump=Decimal("0.2"), orders_per_side8,
        #     #                 min_order_size=Decimal("1.5"), max_position=Decimal("40.0"))
        #     mmaker = MarketMaker(api, tick_jump=Decimal("0.2"), orders_per_side=10,
        #                     min_order_size=Decimal("0.001"), max_position=Decimal("1.0"))
            # mmaker = MarketMaker(api, tick_jump=Decimal("0.2"), orders_per_side=10,
            #                 min_order_size=Decimal("0.2"), max_position=Decimal("1.0"))
        
        # if args.market == "SOL-TEST":
        #     mmaker = MarketMaker(api, tick_jump=Decimal("0.01"), orders_per_side=0,
        #                     min_order_size=Decimal("0.020"), max_position=Decimal("20.0"))

        # if args.market == "BTC-TEST" or args.market == "BTC-PERP":
        #     mmaker = MarketMaker(api, tick_jump=Decimal("1.0"), orders_per_side=10,
        #                     min_order_size=Decimal("0.01"), max_position=Decimal("4.0"))

        if args.market == "BTC" or args.market == "BTC-PERP":
            mmaker = MarketMaker(api, tick_jump=Decimal("2.0"), orders_per_side=50,
                            min_order_size=Decimal("0.01"), max_position=Decimal("18.0"))       

        if args.market == "BTC|y000" or args.market == "BTC|n000":
            mmaker = MarketMaker(api, tick_jump=Decimal("100.0"), orders_per_side=40,
                            min_order_size=Decimal("0.0003"), max_position=Decimal("0.2"), max_diff=0.6, leverage=2)
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

        #bybit_api = ByBitAPI()

        #bitmex_api = BitMEXAPI()
        #huobi_api = HuobiAPI()

        await api.connect()
        await api.subscribe("market_data", {"symbol": args.market})
        await api.subscribe("user_data", {"symbol": args.market})
        api.on_message(mmaker.callback)
        
        # These variables are not referred to anywhere, but an object is being created
        # We're passing the mmaker callbacks
        
        if args.external_market == 'XAU':
            external_api = PythXauAPI()
            external_api.subscribe_index_price(args.external_market)
            external_api.on_message(mmaker.callback)
        else:
            binance_api = BinanceAPI()
            binance_api.subscribe_futures(args.external_market)
            binance_api.on_message(mmaker.callback)

    # await bybit_api.connect()
    # await bybit_api.subscribe()
    # bybit_api.on_message(mmaker.callback)

    # if dex == True:
    #     binance_api.subscribe_futures('ETHUSDT')
    # else:

    # binance_api = BinanceAPI(
    #     os.getenv('BINANCE_KEY'),
    #     os.getenv('BINANCE_SECRET'))

    # await bitmex_api.connect()
    # bitmex_api.on_message(mmaker.callback)

    # await huobi_api.connect()
    # await huobi_api.subscribe()
    # huobi_api.on_message(mmaker.callback)
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
