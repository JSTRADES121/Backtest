from typing import Any, Dict
from datetime import datetime
from collections import Counter
from events import (
    market_update_signal,
    signal_event_signal,
    MarketUpdateEventData,
    SignalEventData
)
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BaseStrategy:
    def generate_signal(self, current_bar: Dict[str, Any], history: Any) -> Dict[str, Any]:
        """
        Base method to generate a signal. To be implemented by subclasses.

        :param current_bar: Current market bar data.
        :param history: Historical market data.
        :return: Signal information for trading.
        """
        return {}


class SMA_CrossoverStrategy(BaseStrategy):
    def __init__(self, instrument: str, size: int = 1000):
        """
        Initializes the SMA Crossover strategy.

        :param instrument: The trading instrument.
        :param size: The size of the position.
        """
        self.instrument = instrument
        self.size = size
        self.current_position = None  # Tracks the current position ('long', 'short', or None)
        self.no_trade_reasons = Counter()  # Tracks reasons why trades were not made

    def generate_signal(self, current_bar: Dict[str, Any], history: Any) -> Dict[str, Any]:
        """
        Generates buy/sell signals based on SMA crossover.

        :param current_bar: Current market bar data.
        :param history: Historical market data (unused in this strategy).
        :return: Signal information for trading.
        """
        logger.debug("Generating SMA Crossover signal with current_bar: %s", current_bar)

        # Extract necessary data
        time = current_bar.get("time")
        close_price = current_bar.get("close")
        sma = current_bar.get("sma")

        # Validate essential data
        if not time or close_price is None or sma is None:
            self.no_trade_reasons["Incomplete data"] += 1
            logger.warning("Incomplete data: time=%s, close_price=%s, sma=%s", time, close_price, sma)
            return {}

        # Parse the time if it's a string
        if isinstance(time, str):
            try:
                time = datetime.fromisoformat(time)
            except ValueError:
                self.no_trade_reasons["Invalid time format"] += 1
                logger.error("Invalid time format in current_bar: %s", time)
                return {}

        signal = {}

        # Generate Buy Signal
        if close_price > sma:
            if self.current_position != 'long':
                signal = {
                    'action': 'buy',
                    'instrument': self.instrument,
                    'size': self.size,
                    'price': close_price,
                    'time': time.isoformat(),
                }
                logger.debug("Generated BUY signal: %s", signal)
                self.current_position = 'long'
            else:
                logger.debug("Already in LONG position. No BUY signal generated.")
        # Generate Short Signal
        elif close_price < sma:
            if self.current_position != 'short':
                signal = {
                    'action': 'short',
                    'instrument': self.instrument,
                    'size': self.size,
                    'price': close_price,
                    'time': time.isoformat(),
                }
                logger.debug("Generated SHORT signal: %s", signal)
                self.current_position = 'short'
            else:
                logger.debug("Already in SHORT position. No SHORT signal generated.")
        # Close Long Position
        elif close_price <= sma and self.current_position == 'long':
            signal = {
                'action': 'sell',
                'instrument': self.instrument,
                'size': self.size,
                'price': close_price,
                'time': time.isoformat(),
            }
            logger.debug("Generated SELL signal to close LONG position: %s", signal)
            self.current_position = None
        # Close Short Position
        elif close_price >= sma and self.current_position == 'short':
            signal = {
                'action': 'cover',
                'instrument': self.instrument,
                'size': self.size,
                'price': close_price,
                'time': time.isoformat(),
            }
            logger.debug("Generated COVER signal to close SHORT position: %s", signal)
            self.current_position = None
        else:
            self.no_trade_reasons["No trade condition met"] += 1
            logger.info("No trade condition met for current market update.")

        return signal

    def get_no_trade_reasons(self) -> Dict[str, int]:
        """
        Returns a summary of why trades were not made.

        :return: Dictionary with reasons and their counts.
        """
        return dict(self.no_trade_reasons)


class Strategy:
    def __init__(self, strategy_impl: BaseStrategy):
        """
        Initializes the strategy handler.

        :param strategy_impl: The strategy implementation to delegate to.
        """
        self.strategy_impl = strategy_impl
        market_update_signal.connect(self.on_market_update)
        logger.debug("Strategy initialized with strategy implementation: %s", type(self.strategy_impl).__name__)

    def generate_signal(self, current_bar: Dict[str, Any], analysis: Any) -> Dict[str, Any]:
        """
        Delegates signal generation to the strategy implementation.

        :param current_bar: Current market bar data.
        :param analysis: Historical analysis data.
        :return: Generated signal.
        """
        logger.debug("Delegating signal generation to strategy implementation.")
        return self.strategy_impl.generate_signal(current_bar=current_bar, history=analysis)

    def on_market_update(self, sender, data: MarketUpdateEventData):
        """
        Handles market updates and sends trading signals if generated.

        :param sender: The event sender.
        :param data: Market update data.
        """
        logger.debug("Received market update: %s", data)
        current_bar = data.data
        history = data.history

        try:
            signal_info = self.strategy_impl.generate_signal(current_bar, history)
        except Exception as e:
            logger.error("Error during signal generation: %s", str(e))
            return

        if signal_info:
            logger.info("Sending signal event: %s", signal_info)
            signal_event_signal.send(self, signal_info=SignalEventData(signal_info=signal_info))
        else:
            logger.info("No signal generated for current market update.")

    def get_no_trade_reasons(self) -> Dict[str, int]:
        """
        Gets no-trade reasons from the strategy implementation.

        :return: No-trade reasons summary.
        """
        if hasattr(self.strategy_impl, "get_no_trade_reasons"):
            return self.strategy_impl.get_no_trade_reasons()
        return {}