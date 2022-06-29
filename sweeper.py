# -*- coding: utf-8 -*-
"""Example Sweeper bot

This module gives an example market-making bot that listens to market-data
feeds from TickSpread for sweeper requests and does arbitrage at an external
exchange (FTX).

All orders sent and updates received are logged at "sweeper.log".

Example:
    To run the bot::
        $ python3 sweeper.py

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
import math
import sys
import os
import argparse
import logging.handlers

from decimal import Decimal
from tickspread_api import TickSpreadAPI

from Secrets import Secrets

from outside_api import FTXAPI
#from outside_api import ByBitAPI, FTXAPI, BinanceAPI, BitMEXAPI, HuobiAPI

parser = argparse.ArgumentParser(
    description='Run a sweeper bot on TickSpread exchange.')
parser.add_argument('--id', dest='id', default="0",
                    help='set the id to run the account (default: 0)')
parser.add_argument('--env', dest='env', default="prod",
                    help='set the env to run the account (default: prod)')
parser.add_argument('--log', dest='log', default="shell",
                    help='set the output for the logs (default: shell)')
parser.add_argument('--tickspread_password', dest='tickspread_password', default="sweeper",
                    help='set the tickspread_password to login (default: sweeper)')
parser.add_argument('--market', dest='market', required=True)
parser.add_argument('--ftx_market', dest='ftx_market', required=True)
parser.add_argument('--money_asset', dest='money_asset', required=True)

args = parser.parse_args()
id = args.id
env = args.env
log_file = args.log
tickspread_password = args.tickspread_password

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s')
if log_file != "shell":
    log_handler = logging.handlers.WatchedFileHandler(
        '/home/ubuntu/store/logs/sweeper_%s.log' % id)
    logger = logging.getLogger()
    logger.removeHandler(logger.handlers[0])
    # logger.addHandler(log_handler)

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

class SweeperState(Enum):
    WAITING = 0
    SWEEPING = 1 	# Order sent to FTX, waiting reply on their websocket
    RETURNING = 2	# Got fill/expiration from FTX, sent sweeper order to Tick, waiting response
    FINISHING = 3 	# Got sweeper_fill, waiting for position update

def order_cancel_to_str(cancel):
    if (cancel == CancelState.NORMAL):
        return " "
    elif (cancel == CancelState.PENDING):
        return "x"
    else:
        return "?"


MAX_CANCEL_RETRIES = 5

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
            return "%s %d/%d @ %d (%d) [%s]%s" % (
                side_to_str(self.side), self.amount_left, self.total_amount,
                self.price if self.price else 0,
                self.clordid if self.clordid else 0,
                order_state_to_str(self.state), order_cancel_to_str(self.cancel))

class Sweeper:
    def __init__(self, api, *,
                ftx_api, ftx_fee=0.0005,
                logger=logging.getLogger(),
                name="sweeper", version="0.0",
                leverage=10, max_position=1.0):
        self.api = api
        self.ftx_api = ftx_api
        
        self.logger = logger
        self.name = name
        self.version = version
        self.leverage = leverage
        self.max_position = max_position
        self.max_amount = Decimal('1.0000')
        
        self.ftx_min_amount = Decimal('0.001')
        self.tick_min_amount = Decimal('0.0001')
        
        self.buy_order = Order(Side.BID, self.logger)
        self.sell_order = Order(Side.ASK, self.logger)
        
        # Market State
        self.last_auction_id = 0		# TODO keep track
        
        # User State
        self.bid_available_limit = Decimal(max_position)
        self.ask_available_limit = Decimal(max_position)
        
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
        
        # PnL
        self.gross_profit = 0
        self.fees_paid = 0
        
        # Parameters
        self.symbol = args.market
        self.money = args.money_asset
        
        self.ftx_market = args.ftx_market
        self.ftx_fee = Decimal(ftx_fee)
        
        # State
        self.real = True
        self.active = False
        self.sweeper_state = SweeperState.WAITING
        
        self.sweeper_side = None
        self.sweeper_max_amount = None
        self.sweeper_limit_price = None
        self.sweeper_order_id = int(time.time() * 100)
        
        self.residual_position = Decimal(0)
        self.tick_position = Decimal(0)
        self.ftx_position = Decimal(0)
        self.max_position = Decimal(max_position)

    def log_new(self, side, amount, price, clordid):
        self.logger.info("->NEW %s %s @ %s (%d)" %
                         (side_to_str(side), amount, price, clordid))

    def log_cancel(self, side, amount_left, price, clordid):
        self.logger.info("->CAN %s %s @ %s (%d)" %
                         (side_to_str(side), amount_left, price, clordid))

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

    def send_new(self, order, amount, price, *, sweeper=0):
        if (order.side == Side.BID):
            assert(self.bid_available_limit > amount)
            self.bid_available_limit -= amount
        elif (order.side == Side.ASK):
            assert(self.ask_available_limit > amount)
            self.ask_available_limit -= amount
        
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
                                  sweeper=sweeper);
    
    def send_cancel(self, order):
        self.log_cancel(order.side, order.amount_left,
                        order.price, order.clordid)

        if (self.real):
            self.register_cancel(order)
            self.api.delete_order(order.clordid, asynchronous=True)

    def _delete_order(self, order):
        if (order.side == Side.BID):
            self.bid_available_limit += order.amount_left
        else:
            self.ask_available_limit += order.amount_left

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
                logging.shutdown()
                sys.exit(1)
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
        order.state = OrderState.ACTIVE

    def exec_remove(self, order):
        if (order.cancel != CancelState.PENDING):
            self.logger.warning("Received unexpected remove_order in id %d",
                                order.clordid)
        self._delete_order(order)

        self.logger.info("Canc: %d, Received: %d",
                         order.auction_id_cancel, self.last_auction_id)

    def find_order_by_clordid(self, clordid):
        if self.buy_order.clordid == clordid:
            return self.buy_order
        if self.sell_order.clordid == clordid:
            return self.sell_order
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
                elif (event == "sweeper_match"):
                    self._trade(order, execution_amount)
                elif (event == "sweeper_fill"):
                    self._trade(order, execution_amount)
                else:
                    logging.warning("Order %d received unknown trade event %s",
                                    order.clordid, event)
        else:
            print("Received %s event" % event)

        # Update Position
        if (side == "bid"):
            self.position += execution_amount
            self.ask_available_limit += execution_amount
        else:
            assert(side == "ask")
            self.position -= execution_amount
            self.bid_available_limit += execution_amount

    def update_ftx_position(self, close_net_difference=False):
        found = False
        print("Verify FTX position")
        initial_time = time.time()
        ftx_positions = self.ftx_api.get_positions()
        for element in ftx_positions:
            if 'future' in element:
                if element['future'] == self.ftx_market:
                    print("FOUND FTX POSITION")
                    print(element)
                    
                    self.ftx_position = Decimal(element['netSize']).quantize(self.ftx_min_amount)
                    
                    print("\nComparing %s @ FTX to %s @ TickSpread" % (self.ftx_market, self.symbol))
                    print("expected:", -self.position, "vs", self.ftx_position, time.time() - initial_time)
                    
                    position_diff = -self.position - self.ftx_position
                    amount = position_diff.quantize(self.ftx_min_amount).copy_abs()
                    
                    if (amount > 0):
                    
                        if (close_net_difference):
                            reduce_order_side = "BUYING" if (position_diff > 0) else "SELLING"
                            side = "buy" if (position_diff > 0) else "sell"

                            answer = input("\nWould you like to close FTX position by\n %s %s %s?\nType YES to proceed: " % (reduce_order_side, position_diff.copy_abs(),  self.ftx_market))
                            if (answer == "YES"):
                                limit_price = input("Type limit price: ")

                                self.ftx_api.place_order(market=self.ftx_market, side=side, price=limit_price,
                                            size=amount, ioc=True)
                                
                                print("Order placed.")
                        
                        print("Exiting")
                        sys.exit(0)
                            
                    
                    found = True
                    break
        
        if (not found):
            assert(self.position.copy_abs() < self.ftx_min_amount)
        
    def find_user_position(self, positions, partial=False):
        for position in positions:
            if (not 'market' in position or
                not 'amount' in position or
                not 'funding' in position or
                not 'entry_price' in position or
                not 'liquidation_price' in position or
                    not 'total_margin' in position):  # ADD or not 'total_price' in position
                logging.warning(
                    "Missing at least one of ['amount', 'funding', 'entry_price', 'liquidation_price', 'total_margin'] in position element")
                continue
            symbol = position['market']
            amount = Decimal(position['amount'])
            funding = Decimal(position['funding'])
            total_margin = Decimal(position['total_margin'])
            entry_price = Decimal(position['entry_price'])
            liquidation_price = Decimal(position['liquidation_price'])

            if (symbol == self.symbol):
                assert(self.position == 0)
                self.position = amount
                self.bid_available_limit -= amount
                self.ask_available_limit += amount

                self.position_total_margin = total_margin
                self.position_funding = funding
                self.position_entry_price = entry_price
                self.position_liquidation_price = liquidation_price
                found_symbol_position = True

                print("Found position. Amount = %s, margin = %s, funding = %s, entry_price = %s, liquidation = %s." % \
                        (self.position, self.position_total_margin, self.position_funding, self.position_entry_price, self.position_liquidation_price))
                
                if (partial):
                    self.update_ftx_position(close_net_difference=True)
                else:
                    assert(self.sweeper_state == SweeperState.FINISHING)
                    if (self.sweeper_state == SweeperState.FINISHING):
                        self.sweeper_state = SweeperState.WAITING
                    self.update_ftx_position()
        return found_symbol_position
        
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
            print("Found %d old orders, leaving" % len(self.old_orders))
            sys.exit(0)
        
        found_symbol_position = self.find_user_position(positions, partial=True)
        
        if (not found_symbol_position):
            logging.warning(
                "Could not find %s position in partial" % self.symbol)
            return
        self.has_user_position = True
        print("Read user_data partial successfully")
    
    def process_sweeper_request(self, data):
        print("SWEEPER_REQUEST found")
        print(data)
        assert(self.sweeper_state == SweeperState.WAITING)
        
        self.sweeper_max_amount = Decimal(data['amount'])
        self.sweeper_limit_price = Decimal(data['price'])
        
        if (data['side'] == 'bid'):
            self.sweeper_side = Side.BID
            #max_amount = min(self.sweeper_max_amount, self.tick_max_position + self.tick_position)
            amount = self.sweeper_max_amount - self.residual_position
        else:
            self.sweeper_side = Side.ASK
            #max_amount = min(self.sweeper_max_amount, self.tick_max_position - self.tick_position)
            amount = self.sweeper_max_amount + self.residual_position
        
        if (amount > self.max_amount):
            amount = self.max_amount
        
        print('amount = %s' % amount)
        amount = amount.quantize(self.ftx_min_amount)
        print('amount = %s' % amount)
        
        if (amount == 0):
            # No need to send any order to FTX.
            # Instead return immediately a complete fill to TickSpread
            
            if (data['side'] == 'bid'):
                return_amount = max(self.residual_position, self.sweeper_max_amount).quantize(self.tick_min_amount)
            else:
                return_amount = max(-self.residual_position, self.sweeper_max_amount).quantize(self.tick_min_amount)

            if (return_amount < 0):
                return_amount = 0
            
            if (data['side'] == 'bid'):
                print("BID")
                # Order side is reversed
                self.send_new(self.sell_order, amount=return_amount, price=self.sweeper_limit_price, sweeper=1)
                #self.send_new(self.sell_order, amount=self.sweeper_max_amount, price=self.sweeper_limit_price, sweeper=1)
                #TODO move position calculation to sweeper_fill processing
                self.residual_position -= return_amount
            else:
                print("ASK")
                # Order side is reversed
                self.send_new(self.buy_order, amount=return_amount, price=self.sweeper_limit_price, sweeper=1)
                #TODO move position calculation to sweeper_fill processing
                self.residual_position += return_amount
            
            self.sweeper_state = SweeperState.RETURNING
            print("residual_position = %s" % self.residual_position)
            print("SWEEPER_ORDER sent")
        else:
            self.sweeper_order_id += 1
            if (data['side'] == 'bid'):
                print("BID")
                buy_limit_price = self.sweeper_limit_price * (1 - self.ftx_fee)
                self.ftx_api.place_order(market=self.ftx_market, side='buy', price=buy_limit_price,
                                        size=amount, ioc=True,
                                        client_id=str(self.sweeper_order_id))
            else:
                print("ASK")
                sell_limit_price = self.sweeper_limit_price * (1 + self.ftx_fee)
                self.ftx_api.place_order(market=self.ftx_market, side='sell',price=sell_limit_price,
                                        size=amount, ioc=True,
                                        client_id=str(self.sweeper_order_id))
            
            self.sweeper_state = SweeperState.SWEEPING
            print("residual_position = %s" % self.residual_position)
            print("EXTERNAL_ORDER sent")

    def process_sweeper_fill(self, data):
        print("SWEEPER FILL found")
        #TODO add more checks in the future
        assert(self.sweeper_state == SweeperState.RETURNING)
        self.sweeper_state = SweeperState.FINISHING

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
        
        #if (topic != "market_data"):
        #    self.logger.info("<-%-10s: %s", "tick", data)
        
        if (topic == "user_data" and event != "partial"):
            # Check for position update
            if 'positions' in data:
                self.find_user_position(data['positions'])
        if (topic == "user_data" and event == "partial"):
            self.tickspread_user_data_partial(payload)
            print("FINISH_OK")
            return 0
        elif (topic == "market_data"):
            return 0
        elif (event == "sweeper_request"):
            self.process_sweeper_request(payload)
            return 0
        elif (event == "acknowledge_order" or event == "maker_order"
              or event == "delete_order" or event == "abort_create"
              or event == "active_order" or event == "reject_order"
              or event == "reject_cancel"):
            print("receive accept: ", event)
            if (not 'client_order_id' in payload):
                self.logger.warning(
                    "No 'client_order_id' in TickSpread %s payload", event)
                return 0
            clordid = int(payload['client_order_id'])
            print("clordid = %d" % clordid)
            self.receive_exec(event, clordid)
            print("fin accept")
        elif (event == "taker_trade" or event == "maker_trade" or
              event == "liquidation" or event == "auto_deleverage" or
              event == "sweeper_fill" or event == "sweeper_match"):
            print("receive trade")
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
            
            if (event == "sweeper_fill"):
                self.process_sweeper_fill(payload)
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
    
    def ftx_callback(self, data):
        if ('channel' in data and data['channel'] == 'orders'):
            print("ftx", data)
            
            if ('data' in data):
                data = data['data']
                
                if ('status' in data and data['status'] == 'closed'):    
                    print('###',data['id'],data['filledSize'],data['avgFillPrice'])
                    if (data['clientId'] == str(self.sweeper_order_id)):
                        print("Found FTX sweeper order")
                        
                        filled_size = Decimal("%.3f" % data['filledSize'])
                        if (filled_size > 0):
                            avg_price = Decimal("%.2f" % data['avgFillPrice'])
                        else:
                            avg_price = self.sweeper_limit_price
                        
                        print(filled_size, self.sweeper_max_amount, filled_size <= self.sweeper_max_amount)
                        
                        assert(self.sweeper_state == SweeperState.SWEEPING)
                        assert(filled_size <= self.max_amount)

                        if (data['side'] == 'buy'):
                            print("buy: residual_position: %s => %s" % (self.residual_position, self.residual_position + filled_size))
                            self.residual_position += filled_size
                            return_amount = min(self.residual_position, self.sweeper_max_amount).quantize(self.tick_min_amount)
                            print("return_amount = %s" % return_amount)
                        else:
                            print("sell: residual_position: %s => %s" % (self.residual_position, self.residual_position - filled_size))
                            self.residual_position -= filled_size
                            return_amount = min(-self.residual_position, self.sweeper_max_amount).quantize(self.tick_min_amount)
                            print("return_amount = %s" % return_amount)
                        
                        if (return_amount < 0):
                            return_amount = 0
                        
                        # TODO correct price! & generate improvement
                        
                        print("Start IF condition")
                        
                        if (data['side'] == 'buy'):
                            assert(self.sweeper_side == Side.BID)
                            assert(self.sweeper_limit_price >= avg_price)
                            
                            print("BID")
                            # Order side is reversed
                            self.send_new(self.sell_order, amount=return_amount, price=self.sweeper_limit_price, sweeper=1)
                            # Todo move position calculation to sweeper_fill processing
                            self.residual_position -= return_amount
                        else:
                            assert(self.sweeper_side == Side.ASK)
                            assert(self.sweeper_limit_price <= avg_price)
                            
                            print("ASK")
                            # Order side is reversed
                            self.send_new(self.buy_order, amount=return_amount, price=self.sweeper_limit_price, sweeper=1)
                            # Todo move position calculation to sweeper_fill processing
                            self.residual_position += return_amount
                        
                        self.sweeper_state = SweeperState.RETURNING
                        print("residual_position = %s" % self.residual_position)
                        print("SWEEPER_ORDER sent")
            else:
                print("NO DATA")
        return 0


    def callback(self, source, raw_data):
        #print("CALLBACK")
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
        return rc

async def main():
    api = TickSpreadAPI(id_multiple=1000, env=env)
    ftx_api = FTXAPI(auth=Secrets())
    sweeper = Sweeper(api, ftx_api=ftx_api)

    api.on_message(sweeper.callback)
    ftx_api.on_message(sweeper.callback)
    
    login_status = api.login('sweeper@tickspread.com', tickspread_password)
    
    if (not login_status):
        asyncio.get_event_loop().stop()
        print("Login Failure")
        return 1
    print(login_status)
    print("STARTING")
    
    await api.connect()
    await api.subscribe("user_data", {"symbol": args.market})
    await api.subscribe("market_data", {"symbol": args.market})

    await ftx_api.connect()
    await ftx_api.login()
    #await ftx_api.subscribe('ticker')
    #await ftx_api.subscribe('orderbook')
    await ftx_api.subscribe('trades', market=args.ftx_market)
    await ftx_api.subscribe('orders', market=args.ftx_market)
    await ftx_api.start_loop()
    
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
