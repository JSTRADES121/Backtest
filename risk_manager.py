from typing import Any, Dict
from events import signal_event_signal, order_request_signal, SignalEventData, OrderRequestEventData


class RiskManager:
    def __init__(
        self,
        base_capital: float = 100000.0,
        max_drawdown: float = 0.2,
        stop_loss_pct: float = 0.01,
        take_profit_pct: float = 0.05,
        max_position_fraction: float = 0.05
    ):
        """
        Initializes trading parameters and listens for strategy signals.

        :param base_capital: Starting capital for the strategy.
        :param max_drawdown: Maximum allowed portfolio drawdown (fraction).
        :param stop_loss_pct: Stop-loss percentage for trades.
        :param take_profit_pct: Take-profit percentage for trades.
        :param max_position_fraction: Max capital fraction per position.
        """
        self.base_capital = base_capital
        self.max_drawdown = max_drawdown
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.max_position_fraction = max_position_fraction
        self.current_drawdown = 0.0
        self.latest_price = {}  # Tracks the latest prices for instruments
        self.no_trade_reasons = {}  # Tracks reasons for not placing trades
        signal_event_signal.connect(self.on_strategy_signal)

    def on_strategy_signal(self, sender, signal_info: SignalEventData):
        """
        Processes incoming signals from the strategy and applies risk rules.

        :param sender: The sender of the signal.
        :param signal_info: SignalEventData containing the signal details.
        """
        if not isinstance(signal_info, SignalEventData):
            raise TypeError(f"Expected SignalEventData, got {type(signal_info)}")

        order = self.apply_risk_rules(signal_info.signal_info)
        if order:
            order_request_signal.send(self, order_details=OrderRequestEventData(order_details=order))
        else:
            # Log no-trade reason if available
            instrument = signal_info.signal_info.get("instrument", "Unknown")
            reason = self.no_trade_reasons.get(instrument, "No reason recorded")
            print(f"RiskManager: No trade for {instrument}. Reason: {reason}")

    def update_price(self, instrument: str, price: float):
        """
        Updates the latest price for an instrument.

        :param instrument: The instrument symbol (e.g., "AAPL").
        :param price: The latest price of the instrument.
        """
        if price > 0:
            self.latest_price[instrument] = price
        else:
            print(f"RiskManager: Invalid price update ignored for {instrument}.")

    def get_current_price(self, instrument: str) -> float:
        """
        Retrieves the latest price for the given instrument.

        :param instrument: The instrument symbol (e.g., "AAPL").
        :return: The latest price or None if not available.
        """
        return self.latest_price.get(instrument)

    def apply_risk_rules(self, signal_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies risk management rules and validates trade parameters.

        :param signal_info: A dictionary containing signal details.
        :return: A validated order dictionary or None.
        """
        # Check drawdown limits
        if self.get_current_drawdown() > self.max_drawdown:
            self.log_no_trade_reason(signal_info, "Drawdown limit exceeded")
            return {}

        action = signal_info.get('action')
        instrument = signal_info.get('instrument')
        signal_price = signal_info.get('price')  # Price from the signal itself
        latest_price = self.get_current_price(instrument)

        # Validate price and instrument
        if not instrument:
            self.log_no_trade_reason(signal_info, "Missing instrument in signal")
            return {}

        # Use signal price if latest price is missing
        price = latest_price if latest_price else signal_price

        if not price or price <= 0:
            self.log_no_trade_reason(signal_info, f"Invalid price: {price}")
            return {}

        # Consistency check between signal price and latest price
        if latest_price and signal_price and abs(latest_price - signal_price) > 0.01 * signal_price:
            self.log_no_trade_reason(signal_info, "Price mismatch between signal and latest price")
            return {}

        # Determine position size based on risk
        capital_risk = self.base_capital * self.max_position_fraction
        size = int(capital_risk / price)

        if size <= 0:
            self.log_no_trade_reason(signal_info, f"Position size too small for price: {price}")
            return {}

        # Validate action type
        if action not in ['buy', 'sell']:
            self.log_no_trade_reason(signal_info, f"Invalid action: {action}")
            return {}

        # Calculate stop-loss and take-profit
        stop_loss = price * (1.0 - self.stop_loss_pct if action == 'buy' else 1.0 + self.stop_loss_pct)
        take_profit = price * (1.0 + self.take_profit_pct if action == 'buy' else 1.0 - self.take_profit_pct)

        # Return validated order
        return {
            'action': action,
            'instrument': instrument,
            'size': size,
            'entry_price': price,
            'stop_loss': stop_loss,
            'take_profit': take_profit
        }

    def log_no_trade_reason(self, signal_info: Dict[str, Any], reason: str):
        """
        Logs reasons for not placing a trade.

        :param signal_info: Signal details that failed validation.
        :param reason: Reason for not placing the trade.
        """
        instrument = signal_info.get('instrument', 'Unknown')
        self.no_trade_reasons[instrument] = reason

    def get_current_drawdown(self) -> float:
        """
        Calculates the current drawdown. Add actual drawdown logic as needed.

        :return: The current portfolio drawdown.
        """
        return self.current_drawdown

    def get_no_trade_reasons(self) -> Dict[str, str]:
        """
        Returns a summary of no-trade reasons.

        :return: A dictionary of reasons for not placing trades by instrument.
        """
        return self.no_trade_reasons