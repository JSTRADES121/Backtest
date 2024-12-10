# events.py

from blinker import Signal
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class MarketDataEventData:
    data: Any

@dataclass
class MarketUpdateEventData:
    data: Any
    history: Any

@dataclass
class SignalEventData:
    signal_info: Dict[str, Any]

@dataclass
class OrderRequestEventData:
    order_details: Dict[str, Any]

@dataclass
class OrderFilledEventData:
    order_data: Dict[str, Any]

@dataclass
class StopTriggeredEventData:
    stop_info: Dict[str, Any]

market_data_signal = Signal('market_data')
market_update_signal = Signal('market_update')
signal_event_signal = Signal('strategy_signal')
order_request_signal = Signal('order_request')
order_filled_signal = Signal('order_filled')
stop_triggered_signal = Signal('stop_triggered')