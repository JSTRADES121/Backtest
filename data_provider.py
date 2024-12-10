import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from datetime import datetime, timedelta
from events import market_data_signal, MarketDataEventData


class DataProvider:
    def __init__(self, mode: str, db_url: str):
        """
        Initializes the DataProvider for backtest or live mode.
        """
        if mode not in ['backtest', 'live']:
            raise ValueError("Invalid mode. Expected 'backtest' or 'live'.")
        self.mode = mode
        self.db_url = db_url
        self.connection = None
        self.start_date = None
        self.end_date = None

        if self.mode == 'backtest':
            self._setup_sql_connection()

    def _setup_sql_connection(self):
        """
        Establishes the SQL connection.
        """
        try:
            engine = sa.create_engine(self.db_url)
            self.connection = engine.connect()
            print("SQL connection established.")
        except SQLAlchemyError as e:
            print(f"Database connection error: {e}")
            raise

    def set_date_range(self, start_date: str, end_date: str):
        """
        Sets the date range for backtesting.
        """
        self.start_date = start_date
        self.end_date = end_date
        print(f"Date range set: {self.start_date} to {self.end_date}")

    def run_backtest_yield(self):
        """
        Streams bars from the `natgas` table for backtesting.
        Ensures each bar is wrapped as MarketDataEventData.
        """
        if not self.connection:
            print("Error: SQL connection not established.")
            return iter([])

        try:
            query = text("""
            SELECT time, open, high, low, close, volume
            FROM natgas_data_cleaned
            WHERE time BETWEEN :start_date AND :end_date
            ORDER BY time ASC
            """)
            result_proxy = self.connection.execution_options(stream_results=True).execute(
                query, {"start_date": self.start_date, "end_date": self.end_date}
            )
            column_names = result_proxy.keys()

            for row in result_proxy:
                # Convert database row into a dictionary
                current_bar = dict(zip(column_names, row))
                
                # Log for debugging unexpected formats
                if not isinstance(current_bar, dict):
                    print(f"Unexpected data format: {type(current_bar)}")
                    continue
                
                # Wrap the bar in MarketDataEventData
                try:
                    wrapped_data = MarketDataEventData(data=current_bar)
                except Exception as wrap_error:
                    print(f"Error wrapping data: {wrap_error}, Data: {current_bar}")
                    continue
                
                # Emit signal and yield the wrapped data
                try:
                    market_data_signal.send(self, data=wrapped_data)
                except Exception as signal_error:
                    print(f"Error sending market_data_signal: {signal_error}, Data: {wrapped_data}")
                    continue

                yield wrapped_data

        except SQLAlchemyError as e:
            print(f"SQL query error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            if self.connection:
                self.connection.close()
                print("SQL connection closed.")

    def start(self):
        """
        Starts the data provider.
        """
        if self.mode == 'backtest':
            print("Backtest mode activated. Use `run_backtest_yield` to get data bars.")
        else:
            raise NotImplementedError("Live mode is not implemented.")

    def __repr__(self):
        return f"<DataProvider mode={self.mode}>"