from typing import Any, Dict
from events import (
    order_request_signal, order_filled_signal,
    OrderRequestEventData, OrderFilledEventData
)
from datetime import datetime
import logging

logger = logging.getLogger("OrderHandler")


class OrderHandler:
    def __init__(self, live: bool = False, api: Any = None, capital: float = 100000, position_size: float = 1000):
        """
        Initializes the OrderHandler with dynamic position sizing and capital validation.

        :param live: Whether the handler is in live trading mode.
        :param api: API instance for live trading (if applicable).
        :param capital: Total capital available for trading.
        :param position_size: Default static position size for each order.
        """
        self.live = live
        self.api = api
        self.capital = capital  # Total capital available
        self.position_size = position_size  # Default position size
        self.open_positions = {}  # Tracks open positions by instrument
        self.realized_pnl = 0.0  # Realized PnL from closed positions
        order_request_signal.connect(self.on_order_request)

    def on_order_request(self, sender, order_details: OrderRequestEventData):
        """
        Handles incoming order requests and executes the trade.

        :param sender: The sender of the signal.
        :param order_details: Details of the order request.
        """
        try:
            order = order_details.order_details
            logger.debug(f"Received order request: {order}")

            # Validate required fields
            required_keys = {'instrument', 'action', 'entry_price', 'stop_loss', 'take_profit'}
            if not required_keys.issubset(order.keys()):
                missing_keys = required_keys - order.keys()
                logger.error(f"Missing required fields in order: {missing_keys}")
                return

            # Adjust position size dynamically based on capital
            order['size'] = self.calculate_position_size(order['entry_price'])
            if order['size'] == 0:
                logger.warning(f"Insufficient capital to place order: {order}")
                return

            # Execute and update positions
            filled_order = self.execute_order(order)
            self.update_positions(filled_order)

            # Emit order-filled signal
            order_filled_signal.send(self, order_data=OrderFilledEventData(order_data=filled_order))

        except Exception as e:
            logger.error(f"Error processing order request: {e}")

    def execute_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the given order in live or simulated mode.

        :param order: The order details.
        :return: Filled order details.
        """
        try:
            fill_price = order['entry_price']  # Use entry price as fill price
            filled_order = {
                'instrument': order['instrument'],
                'filled_price': fill_price,
                'size': order['size'],
                'time': datetime.utcnow().isoformat(),
                'action': order['action'],
                'stop_loss': order['stop_loss'],
                'take_profit': order['take_profit']
            }
            logger.debug(f"Order executed: {filled_order}")
            return filled_order

        except Exception as e:
            logger.error(f"Error executing order: {e}")
            raise

    def update_positions(self, filled_order: Dict[str, Any]):
        """
        Updates open positions after an order is filled.

        :param filled_order: Details of the filled order.
        """
        instrument = filled_order['instrument']
        action = filled_order['action']
        size = filled_order['size']
        filled_price = filled_order['filled_price']

        if action == 'buy':
            if instrument not in self.open_positions:
                self.open_positions[instrument] = {
                    'entry_price': filled_price,
                    'size': size,
                    'stop_loss': filled_order['stop_loss'],
                    'take_profit': filled_order['take_profit']
                }
            else:
                # Average the entry price for multiple buys
                existing_position = self.open_positions[instrument]
                total_cost = existing_position['entry_price'] * existing_position['size'] + filled_price * size
                total_size = existing_position['size'] + size
                self.open_positions[instrument]['entry_price'] = total_cost / total_size
                self.open_positions[instrument]['size'] = total_size
        elif action == 'sell':
            if instrument in self.open_positions:
                existing_position = self.open_positions[instrument]
                pnl = (filled_price - existing_position['entry_price']) * size
                self.realized_pnl += pnl
                existing_position['size'] -= size
                if existing_position['size'] <= 0:
                    del self.open_positions[instrument]

        logger.debug(f"Updated positions: {self.open_positions}")
        logger.info(f"Realized PnL: {self.realized_pnl}")

    def calculate_position_size(self, entry_price: float) -> int:
        """
        Calculates the position size based on available capital.

        :param entry_price: The price of the instrument.
        :return: The position size.
        """
        max_position_value = self.capital * 0.1  # Max 10% of capital per trade
        position_size = int(max_position_value / entry_price)
        return min(position_size, self.position_size)

    def get_open_positions(self) -> Dict[str, Any]:
        """
        Returns the current open positions.

        :return: A dictionary of open positions.
        """
        return self.open_positions

    def get_realized_pnl(self) -> float:
        """
        Returns the realized PnL.

        :return: The realized PnL.
        """
        return self.realized_pnl