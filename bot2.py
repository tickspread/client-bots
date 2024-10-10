# -*- coding: utf-8 -*-

import json
import argparse
from decimal import Decimal, ROUND_DOWN, ROUND_UP

import asyncio
import logging
from enum import Enum

import time
import sys
import logging.handlers

from tickspread_api import TickSpreadAPI
from outside_api import BinanceAPI, PythXauAPI

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

"""
Order Lifecycle:
    - EMPTY: The slot is available for placing a new order.
    - PENDING: An order has been sent to the exchange and is awaiting acknowledgment.
    - ACKED: The exchange has acknowledged the order; it may not yet be visible in the order book.
    - MAKER: The order is now visible in the order book as a maker order.
    - ACTIVE: The order is participating in an active auction, and may execute with price improvement

State Transitions (normal)
    EMPTY -> PENDING: When a new order is sent to the exchange.
    PENDING -> EMPTY: If an instant error occurs or an abort_order event is received.
    PENDING -> ACKED: Upon receiving an acknowledge_order event from the exchange.
    ACKED -> MAKER: Upon receiving a maker_order event indicating the order is placed in the order book.
    ACKED -> EMPTY: If the order is fully executed or canceled before becoming MAKER.
    MAKER -> EMPTY: If the order is executed fully or cancelled after becoming MAKER.

State Transitions (during active auctions):
    ACKED -> ACTIVE: Upon receiving an active_order event
    MAKER -> ACTIVE: Upon receiving an active_order event
    ACTIVE -> EMPTY: If the order is fully executed or is cancelled after the auction
    ACTIVE -> MAKER: If the order is not fully executed and goes/returns to orderbook
"""

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
        self.log_recalculation(current_time)

        if self.should_debug_orders(current_time):
            self.debug_orders()
            self.last_status_time = current_time

        price_increment = self.get_price_increment()
        price = self.top_price

        # Initialize counters
        active_order_count = 0
        total_liquidity = Decimal(0)
        pending_cancel_liquidity = Decimal(0)

        # Iterate through all order slots in the circular buffer
        for i in range(self.max_orders):
            index = self.get_order_index(i)
            order = self.orders[index]

            # Calculate liquidity metrics for the current price level
            delta_ticks, expected_liquidity = self.calculate_liquidity_metrics(price, price_increment)
            liquidity_deltas = self.compute_liquidity_deltas(expected_liquidity, total_liquidity, pending_cancel_liquidity)

            # Log liquidity calculations for debugging
            self.log_liquidity_metrics(price, delta_ticks, expected_liquidity, liquidity_deltas)

            # Handle order cancellations based on liquidity conditions
            self.handle_order_cancellations(order, price, liquidity_deltas, active_order_count)

            # Place new orders if necessary
            self.place_new_orders_if_needed(order, liquidity_deltas, price, i)

            # Update liquidity counters based on current order state
            active_order_count, total_liquidity, pending_cancel_liquidity = self.update_liquidity_counters(
                order, active_order_count, total_liquidity, pending_cancel_liquidity
            )

            # Check if liquidity or order count limits have been reached
            if self.has_reached_limits(total_liquidity, active_order_count):
                break

            # Move to the next price level
            price += price_increment

    def log_recalculation(self, current_time):
        """Logs the start of the order recalculation process."""
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

    def should_debug_orders(self, current_time):
        """Determines whether to perform a debug of orders based on time elapsed."""
        return (current_time - self.last_status_time > 1.0)

    def get_price_increment(self):
        """Determines the price increment direction based on the order side."""
        return -self.tick_jump if self.side == Side.BID else self.tick_jump

    def get_order_index(self, iteration):
        """Calculates the order index in the circular buffer based on the iteration."""
        return (self.top_order + iteration) % self.max_orders

    def calculate_liquidity_metrics(self, price, price_increment):
        """
        Calculates delta_ticks and expected_liquidity for the current price level.
        
        delta_ticks: Represents the distance from the fair price in ticks.
        expected_liquidity: The cumulative liquidity expected at this price level.
        """
        delta_ticks = (price - self.parent.fair_price) / price_increment
        delta_ticks = delta_ticks.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

        # Calculate expected liquidity based on the liquidity curve
        expected_liquidity = self.parent.avg_tick_liquidity * delta_ticks
        expected_liquidity = min(expected_liquidity, self.parent.max_liquidity)
        return delta_ticks, expected_liquidity

    def compute_liquidity_deltas(self, expected_liquidity, total_liquidity, pending_cancel_liquidity):
        """
        Computes the differences between expected liquidity and current liquidity.
        
        liquidity_excess (high): Excess liquidity that may require order cancellations.
        liquidity_needed (low): Additional liquidity needed to meet the 'low' curve.
        liquidity_min_threshold (minimum): Minimum liquidity threshold to maintain.
        """
        liquidity_excess = expected_liquidity - (total_liquidity - pending_cancel_liquidity)
        liquidity_needed = expected_liquidity * self.parent.liquidity_curve_hysteresis_low - total_liquidity
        liquidity_min_threshold = (
            expected_liquidity * self.parent.liquidity_curve_hysteresis_minimum
            - total_liquidity
            - self.parent.avg_tick_liquidity
        )
        return {
            'excess': liquidity_excess,
            'needed': liquidity_needed,
            'min_threshold': liquidity_min_threshold
        }

    def log_liquidity_metrics(self, price, delta_ticks, expected_liquidity, liquidity_deltas):
        """Logs the calculated liquidity metrics for debugging purposes."""
        self.parent.logger.debug(f"Price: {price}, Fair Price: {self.parent.fair_price}, Delta Ticks: {delta_ticks}")
        self.parent.logger.debug(
            f"Expected Liquidity: {expected_liquidity}, "
            f"Liquidity Excess: {liquidity_deltas['excess']}, "
            f"Liquidity Needed: {liquidity_deltas['needed']}, "
            f"Liquidity Min Threshold: {liquidity_deltas['min_threshold']}"
        )

    def handle_order_cancellations(self, order, price, liquidity_deltas, active_order_count):
        """
        Determines whether to cancel an order based on current liquidity conditions.

        - Cancels orders that are:
            1. Priced incorrectly (price mismatch).
            2. Exceeding the allowed liquidity at their price level.
            3. Exceeding the target number of active orders.
        - Additionally, cancels a single order if liquidity falls below the minimum threshold
        and no other cancellations are pending, allowing the bot to replenish liquidity.
        """
        if order.state != OrderState.EMPTY and order.cancel == CancelState.NORMAL:
            if (
                order.price != price
                or order.amount_left > liquidity_deltas['excess']
                or active_order_count >= self.target_num_orders
            ):
                # Normal cancellation due to wrong price, too much liquidity, or too many orders
                self.parent.send_cancel(order)
            elif (
                liquidity_deltas['min_threshold'] > order.amount_left
                and liquidity_deltas['excess'] >= 0  # Ensure no pending cancellations
                and order.amount_left < self.parent.max_order_size
            ):
                # Cancellation due to too little liquidity, cancels a single order to allow sending later one
                self.parent.send_cancel(order)

    def place_new_orders_if_needed(self, order, liquidity_deltas, price, i):
        """
        Places new orders if the liquidity needed exceeds the minimum order size.
        
        - Determines the size of the new order based on liquidity_needed and available_limit.
        - Ensures the new order size respects the minimum order size constraint.
        """
        if order.state == OrderState.EMPTY and liquidity_deltas['needed'] > self.min_order_size:
            size = min(liquidity_deltas['needed'], self.available_limit)
            size = min(size, self.parent.max_order_size)
            # Round down the size to the precision of min_order_size, but don't force it to be a multiple
            rounded_size = self.round_down_to_precision(size, self.min_order_size)
            self.parent.logger.info("Found empty order %d, will send NEW with size %d", self.get_order_index(i), rounded_size)
            if rounded_size >= self.min_order_size:
                self.parent.send_new(order, rounded_size, price)

    def update_liquidity_counters(self, order, active_order_count, total_liquidity, pending_cancel_liquidity):
        """
        Updates the liquidity counters based on the current order state.
        
        - Increments active_order_count for non-empty orders.
        - Adds to total_liquidity based on the order's remaining amount.
        - Tracks liquidity tied up in pending cancellations.
        """
        if order.state != OrderState.EMPTY:
            active_order_count += 1
            total_liquidity += Decimal(str(order.amount_left))
            if order.cancel == CancelState.PENDING:
                pending_cancel_liquidity += Decimal(str(order.amount_left))
        return active_order_count, total_liquidity, pending_cancel_liquidity

    def has_reached_limits(self, total_liquidity, active_order_count):
        """
        Checks if the bot has reached the maximum liquidity or target number of orders.
        
        - Returns True if either condition is met, signaling to stop placing further orders.
        """
        return (
            total_liquidity + self.min_order_size >= self.parent.max_liquidity
            or active_order_count >= self.target_num_orders
        )

class MarketMaker:
    def __init__(self, api, market, money_asset, *, logger=logging.getLogger(),
                 name="bot_example", version="0.0",
                 orders_per_side=8, max_position=400, tick_jump=10, min_order_size=0.5,
                 order_leverage=50, target_leverage=10,
                 max_diff = 0.004, max_liquidity = -1, max_order_size=10.0):
        """
        Initializes the MarketMaker with a circular buffer to manage orders.

        Args:
            max_orders (int): The maximum number of orders the bot can hold.
            ...
        """
        
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

        # PnL
        self.gross_profit = 0
        self.fees_paid = 0

        # Parameters
        self.tick_jump = Decimal(tick_jump)
        self.min_order_size = Decimal(min_order_size)
        self.max_order_size = Decimal(max_order_size)
        self.order_leverage = Decimal(order_leverage)
        self.target_leverage = Decimal(target_leverage)
        self.symbol = market
        self.money = money_asset
        self.max_diff = Decimal(str(max_diff))
        self.max_position = Decimal(str(max_position))
        if (max_liquidity >= 0):
            self.max_liquidity = Decimal(str(max_liquidity))
        else:
            self.max_liquidity = Decimal(str(max_position))
        
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
                                  leverage=self.order_leverage,
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
        """
        Handles the acknowledgment of an order sent to the exchange.

        Args:
            order_id (str): The unique identifier of the order.
            details (dict): Additional details about the order acknowledgment.

        Process:
            - Locate the order in the circular buffer using order_id.
            - Update the order state from PENDING to ACKED.
            - If the acknowledgment indicates an error, revert the order state to EMPTY.
        """
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
        """
        Handles the removal or cancellation of an order.

        Args:
            order_id (str): The unique identifier of the order.
            details (dict): Additional details about the order removal.

        Process:
            - Locate the order in the circular buffer using order_id.
            - If successfully removed, set state to EMPTY.
        """

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
    
    def quiver_market_data_partial(self, payload):
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

    def quiver_user_data_partial(self, payload):
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

    def quiver_callback(self, data):
        """
        Callback method to handle events received from the Quiver (TickSpread) feed.

        Args:
            data (dict): The data payload containing event information.

        Process:
            - Validates the presence of required keys ('event', 'payload', 'topic').
            - Dispatches the event to the appropriate handler based on the event type.
            - Handles different event types, updating order states and managing liquidity accordingly.
        """
        # Validate that the necessary keys are present in the data
        if not self.has_required_keys(data):
            return 1  # Early exit if required keys are missing

        event = data['event']
        payload = data['payload']
        topic = data['topic']

        print("EVENT: ", event)

        # Handle 'partial' events based on the topic
        if event == "partial":
            self.handle_partial_event(topic, payload)
            return 0  # Early exit after handling partial event

        # Handle 'update' events
        if event == "update":
            self.handle_update_event(payload)

        # Handle various order-related events
        elif event in {"acknowledge_order", "maker_order", "delete_order", "abort_create",
                    "active_order", "reject_order", "reject_cancel"}:
            self.handle_order_event(event, payload)

        # Handle trade-related events
        elif event in {"taker_trade", "maker_trade", "liquidation", "auto_deleverage"}:
            self.handle_trade_event(event, payload)
        
        elif event == "balance":
            self.handle_balance_event(event, payload)
        
        elif event == "update_position":
            self.handle_position_event(event, payload)

        # Handle other events that do not require specific actions
        elif event == "trade":
            pass  # No action needed for these events

        # Handle unknown or unhandled events
        else:
            self.logger.warning("UNKNOWN EVENT: %s", event)
            self.logger.warning(data)

        return 0  # Indicate successful handling of the event

    def has_required_keys(self, data):
        required_keys = ['event', 'payload', 'topic']
        for key in required_keys:
            if key not in data:
                self.logger.warning("No '%s' in TickSpread message", key)
                return False
        return True

    def handle_partial_event(self, topic, payload):
        """
        Handles 'partial' events based on the topic.

        Args:
            topic (str): The topic of the partial event ('user_data' or 'market_data').
            payload (dict): The payload of the event.

        Process:
            - If topic is 'user_data', process partial user data and cancel old orders.
            - If topic is 'market_data', process partial market data.
        """
        if topic == "user_data":
            self.quiver_user_data_partial(payload)
            self.cancel_old_orders()  # Cancel outdated orders to maintain liquidity curves
        elif topic == "market_data":
            self.quiver_market_data_partial(payload)

    def handle_update_event(self, payload):
        """
        Handles 'update' events, which may include auction and execution band updates.

        Args:
            payload (dict): The payload of the 'update' event.

        Process:
            - Validates the presence of 'auction_id'.
            - Ensures auction IDs are sequential to maintain order consistency.
            - Updates execution bands if present to adjust liquidity parameters.
        """
        if 'auction_id' not in payload:
            self.logger.warning("No 'auction_id' in TickSpread update payload")
            return

        auction_id = int(payload['auction_id'])

        # Verify that auction_id is sequential to prevent inconsistencies
        if self.last_auction_id and auction_id != self.last_auction_id + 1:
            self.logger.warning("Received auction_id = %d, last was %d", auction_id, self.last_auction_id)
            return
        self.last_auction_id = auction_id

        # Update execution bands if provided in the payload
        if 'execution_band' in payload:
            execution_band = payload['execution_band']
            if 'high' not in execution_band:
                self.logger.warning("No 'high' in execution_band")
                return
            if 'low' not in execution_band:
                self.logger.warning("No 'low' in execution_band")
                return
            self.execution_band_high = Decimal(execution_band['high'])
            self.execution_band_low = Decimal(execution_band['low'])

    def handle_order_event(self, event, payload):
        if 'client_order_id' not in payload:
            self.logger.warning("No 'client_order_id' in TickSpread %s payload", event)
            return

        clordid = int(payload['client_order_id'])
        self.receive_exec(event, clordid)  # Update order state based o
        """
        Handles 'update' events, which may include auction and execution band updates.

        Args:
            payload (dict): The payload of the 'update' event.

        Process:
            - Validates the presence of 'auction_id'.
            - Ensures auction IDs are sequential to maintain order consistency.
            - Updates execution bands if present to adjust liquidity parameters.
        """
    
    def handle_balance_event(self, payload):
        print("BALANCE EVENT")
        pass

    def handle_trade_event(self, event, payload):
        clordid = payload.get('client_order_id')
        if clordid is None:
            self.logger.warning("No 'client_order_id' in TickSpread %s payload", event)
            clordid = 0
        else:
            clordid = int(clordid)

        if 'execution_amount' not in payload:
            self.logger.warning("No 'execution_amount' in TickSpread %s payload", event)
            return
        if 'side' not in payload:
            self.logger.warning("No 'side' in TickSpread %s payload", event)
            return

        execution_amount = Decimal(payload['execution_amount'])
        side = payload['side']

        self.receive_exec_trade(event, clordid, execution_amount, side)  # Update trade execution details

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
            rc = self.quiver_callback(data)
        elif (source == 'ftx'):
            rc = self.ftx_callback(data)
        elif (source == 'binance-s'):
            rc = self.binance_s_callback(data)
        elif (source == 'pyth'):
            rc = self.pyth_xau_callback(data)
        return rc

def load_json_file(file_path):
    """
    Loads a JSON file and returns the parsed data.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict: Parsed JSON data.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file {file_path} not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {file_path}: {e}")
        sys.exit(1)

def setup_logging(config_logging, log_output, log_level_override=None):
    """
    Configures the logging based on the configuration and command line arguments.

    Args:
        config_logging (dict): Logging configuration from config.json.
        log_output (str): Log output destination from command line argument.
        log_level_override (str, optional): Log level override from command line argument.
    """
    log_level = getattr(logging, log_level_override.upper(), config_logging.get('level', 'INFO')) if log_level_override else config_logging.get('level', 'INFO')
    log_format = config_logging.get('format', '%(asctime)s %(levelname)-8s %(message)s')

    logging.basicConfig(level=log_level, format=log_format)

    if log_output != "shell":
        try:
            log_handler = logging.handlers.WatchedFileHandler(log_output)
            logger = logging.getLogger()
            logger.removeHandler(logger.handlers[0])  # Remove default handler
            logger.addHandler(log_handler)
        except Exception as e:
            logging.error(f"Failed to set log file handler: {e}")
            sys.exit(1)

def parse_arguments():
    """
    Parses command line arguments and returns them.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Run a market maker bot on TickSpread exchange.'
    )
    parser.add_argument('--id', dest='id', default=None,
                        help='Set the id to run the account (default from config)')
    parser.add_argument('--env', dest='env', default=None,
                        help='Set the environment to run the account (default from config)')
    parser.add_argument('--log', dest='log', default=None,
                        help='Set the output for the logs (default from config)')
    parser.add_argument('--log_level', dest='log_level', default=None,
                        help='Set the logging level (default from config)')
    parser.add_argument('--tickspread_password', dest='tickspread_password', default=None,
                        help='Set the TickSpread password to login (overrides secrets.json)')
    parser.add_argument('--market', dest='market', default=None,
                        help='Set the market to run the bot on (default from config)')
    parser.add_argument('--money_asset', dest='money_asset', default=None,
                        help='Set the money asset (default from config)')

    return parser.parse_args()

async def main():
    # Parse command line arguments
    args = parse_arguments()

    # Load configurations
    config = load_json_file('config.json')

    # Override configurations with command line arguments if provided
    general_config = config.get('general', {})
    general_config['id'] = args.id if args.id is not None else general_config.get('id', '0')
    general_config['env'] = args.env if args.env is not None else general_config.get('env', 'prod')
    general_config['market'] = args.market if args.market is not None else general_config.get('market', 'ETH')
    general_config['money_asset'] = args.money_asset if args.money_asset is not None else general_config.get('money_asset', 'USD')

    # Override tickspread_password from command line or secrets.json
    tickspread_password = args.tickspread_password if args.tickspread_password is not None else load_json_file('secrets.json').get('tickspread_password', 'maker')

    # Setup logging
    logging_config = config.get('logging', {})
    setup_logging(logging_config, log_output=args.log if args.log else logging_config.get('file', 'shell'),
                 log_level_override=args.log_level)

    # Initialize TickSpreadAPI
    api = TickSpreadAPI(id_multiple=1000, env=general_config['env'])

    # Initialize MarketMaker based on the selected market
    market = general_config['market']
    market_settings = config.get('market_settings', {}).get(market)

    if not market_settings:
        logging.error(f"Market settings for '{market}' not found in config.json.")
        sys.exit(1)

    # Convert string parameters to appropriate types
    try:
        tick_jump = Decimal(market_settings['tick_jump'])
        orders_per_side = int(market_settings['orders_per_side'])
        min_order_size = Decimal(market_settings['min_order_size'])
        max_position = Decimal(market_settings['max_position'])
        max_order_size = Decimal(market_settings.get('max_order_size')) if 'max_order_size' in market_settings else None
        max_liquidity = Decimal(market_settings.get('max_liquidity')) if 'max_liquidity' in market_settings else None
        max_diff = float(market_settings.get('max_diff')) if 'max_diff' in market_settings else None
        order_leverage = int(market_settings.get('order_leverage')) if 'order_leverage' in market_settings else None
        target_leverage = int(market_settings.get('target_leverage')) if 'target_leverage' in market_settings else None
    except (KeyError, ValueError) as e:
        logging.error(f"Invalid market settings for '{market}': {e}")
        sys.exit(1)

    # Initialize MarketMaker with the appropriate parameters
    mmaker_params = {
        'tick_jump': tick_jump,
        'orders_per_side': orders_per_side,
        'min_order_size': min_order_size,
        'max_position': max_position
    }

    if max_order_size is not None:
        mmaker_params['max_order_size'] = max_order_size
    if max_liquidity is not None:
        mmaker_params['max_liquidity'] = max_liquidity
    if max_diff is not None:
        mmaker_params['max_diff'] = max_diff
    if order_leverage is not None:
        mmaker_params['order_leverage'] = order_leverage
    if target_leverage is not None:
        mmaker_params['target_leverage'] = target_leverage

    mmaker = MarketMaker(api, market, general_config['money_asset'], **mmaker_params)

    # Register and login to TickSpread API
    logging.info("LOGIN")
    login_status = api.login(f'maker{general_config["id"]}@tickspread.com', tickspread_password)
    if not login_status:
        logging.error("Login Failure")
        asyncio.get_event_loop().stop()
        return 1
    logging.info("STARTING")

    # Connect to TickSpread API and subscribe to necessary feeds
    await api.connect()
    await api.subscribe("market_data", {"symbol": market})
    await api.subscribe("user_data", {"symbol": market})
    api.on_message(mmaker.callback)

    # Initialize and subscribe to external market APIs
    external_market = market_settings['external_market']
    if external_market == 'XAU':
        external_api = PythXauAPI()
        external_api.subscribe_index_price(external_market)
        external_api.on_message(mmaker.callback)
    else:
        binance_api = BinanceAPI()
        binance_api.subscribe_futures(external_market)
        binance_api.on_message(mmaker.callback)

    logging.info("FINISH INIT")

    # Keep the bot running
    while True:
        await asyncio.sleep(1)

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
