import logging
from events import (
    market_data_signal, market_update_signal, signal_event_signal,
    order_request_signal, order_filled_signal, stop_triggered_signal
)

logger = logging.getLogger('trading_system')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def log_event(event_type: str, payload: dict):
    logger.debug(f"Event: {event_type}, Payload: {payload}")

market_data_signal.connect(lambda sender, data: log_event('market_data', data.data))
market_update_signal.connect(lambda sender, data: log_event('market_update', {'data': data.data}))
signal_event_signal.connect(lambda sender, signal_info: log_event('strategy_signal', signal_info.signal_info))
order_request_signal.connect(lambda sender, order_details: log_event('order_request', order_details.order_details))
order_filled_signal.connect(lambda sender, order_data: log_event('order_filled', order_data.order_data))
stop_triggered_signal.connect(lambda sender, stop_info: log_event('stop_triggered', stop_info.stop_info))