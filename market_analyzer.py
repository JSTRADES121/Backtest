import pandas as pd
from events import (
    market_data_signal,
    market_update_signal,
    MarketDataEventData,
    MarketUpdateEventData,
)


class MarketAnalyzer:
    def __init__(self, indicator_window: int = 10, risk_manager=None):
        """
        Initializes the MarketAnalyzer with a rolling window for indicators
        and an optional RiskManager instance.

        :param indicator_window: Rolling window size for indicators.
        :param risk_manager: Instance of RiskManager to integrate price updates.
        """
        self.indicator_window = indicator_window
        self.buffer = []  # Buffer to hold recent bars for rolling calculations
        self.risk_manager = risk_manager
        market_data_signal.connect(self.on_market_data)

    def on_market_data(self, sender, data: MarketDataEventData):
        """
        Handles incoming market data, validates it, and processes it.

        :param sender: The sender of the signal.
        :param data: MarketDataEventData containing the market data.
        """
        if not isinstance(data, MarketDataEventData):
            raise TypeError(f"Expected MarketDataEventData, got {type(data)}")

        self.buffer.append(data.data)

        # Ensure the buffer has enough data to calculate indicators
        if len(self.buffer) < self.indicator_window:
            history = self.buffer
        else:
            history = self.buffer[-self.indicator_window:]

        enriched_bar = self.process_data()
        if enriched_bar:
            if self.risk_manager:
                # Update the RiskManager with the latest price
                current_price = enriched_bar.get("close")
                instrument = enriched_bar.get("instrument")
                if instrument and current_price:
                    self.risk_manager.update_price(instrument, current_price)

            # Send the enriched bar along with the historical buffer to the next stage
            market_update_signal.send(
                self,
                data=MarketUpdateEventData(data=enriched_bar, history=history),
            )
        return enriched_bar

    def process_data(self) -> dict:
        """
        Processes buffered data to compute indicators and enrich the latest bar.

        :return: The most recent bar enriched with calculated indicators.
        """
        if len(self.buffer) < self.indicator_window:
            # Return the latest bar without indicators if the buffer is insufficient
            return self.buffer[-1]

        # Convert buffer to DataFrame for calculations
        df = pd.DataFrame(self.buffer)

        # Validate if necessary columns exist in the data
        if 'close' not in df.columns:
            raise KeyError("Missing 'close' column in market data for indicator calculations.")

        # Compute rolling indicators
        df['sma'] = df['close'].rolling(window=self.indicator_window).mean()

        # Return the most recent enriched bar
        enriched_bar = df.iloc[-1].to_dict()
        return enriched_bar