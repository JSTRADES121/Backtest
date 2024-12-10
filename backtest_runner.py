from data_provider import DataProvider
from market_analyzer import MarketAnalyzer
from strategy import Strategy, SMA_CrossoverStrategy
from risk_manager import RiskManager
from order_handler import OrderHandler
from portfolio_manager import PortfolioManager
from logging_utils import logger
import time
from events import MarketDataEventData


class BacktestRunner:
    def __init__(self, db_url: str, table_name: str, base_capital: float = 100000.0):
        """
        Initializes the BacktestRunner.

        :param db_url: Database connection string.
        :param table_name: Name of the table containing market data.
        :param base_capital: Starting capital for the strategy.
        """
        if not db_url or not table_name:
            raise ValueError("Both db_url and table_name must be provided.")

        self.db_url = db_url
        self.table_name = table_name
        self.base_capital = base_capital
        self._setup_components()

    def _setup_components(self):
        """
        Initializes shared components required for backtesting.
        """
        logger.info("Initializing components for backtesting...")
        try:
            self.portfolio = PortfolioManager(starting_cash=self.base_capital)
            self.risk_mgr = RiskManager(base_capital=self.base_capital)
            strategy_impl = SMA_CrossoverStrategy(instrument=self.table_name, size=1000)            
            self.strategy = Strategy(strategy_impl=strategy_impl)
            self.market_analyzer = MarketAnalyzer(indicator_window=50, risk_manager=self.risk_mgr)
            self.data_provider = DataProvider(mode='backtest', db_url=self.db_url)
            self.order_handler = OrderHandler()

            logger.info("Shared components initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise

    def process_bar(self, bar):
        """
        Processes a single market data bar with detailed debugging.
        """
        try:
            logger.debug(f"Processing bar: {bar}")

            if not isinstance(bar, MarketDataEventData):
                raise ValueError(f"Expected MarketDataEventData, got {type(bar)}")

            if not bar.data:
                logger.error("MarketDataEventData has no data.")
                return

            if 'close' not in bar.data or bar.data['close'] is None:
                logger.warning(f"Missing 'close' price in bar: {bar.data}")
                return

            # Step 1: Analyze market data
            logger.debug("Analyzing market data...")
            analysis = self.market_analyzer.on_market_data(sender=self, data=bar)
            logger.debug(f"Market analysis completed. Result: {analysis}")

            # Step 2: Generate trading signal
            logger.debug("Generating trading signal...")
            signal = self.strategy.generate_signal(current_bar=bar.data, analysis=analysis)
            if not signal:
                logger.debug("No trading signal generated for the current bar.")
                return
            logger.debug(f"Generated signal: {signal}")

            # Step 3: Apply risk rules
            logger.debug("Applying risk management rules...")
            order = self.risk_mgr.apply_risk_rules(signal)
            if not order:
                logger.debug("Risk management rejected the signal. No order created.")
                return
            logger.debug(f"Order passed risk management: {order}")

            # Step 4: Execute order
            logger.debug("Executing order...")
            execution_result = self.order_handler.execute_order(order)
            if not execution_result:
                logger.error("Order execution failed.")
            else:
                logger.debug(f"Order executed successfully: {execution_result}")

        except Exception as e:
            logger.error(f"Error processing bar: {e}")
            logger.debug(f"Problematic bar data: {bar}")

    def run_backtest(self):
        """
        Executes the backtest by processing data sequentially.
        """
        logger.info("Starting backtest...")
        start_time = time.time()

        try:
            # Set the fixed date range for backtesting (22nd and 23rd September 2022)
            start_date = "2023-04-20"
            end_date = "2023-04-23"
            self.data_provider.set_date_range(start_date, end_date)

            # Iterate through data bars provided by DataProvider
            for bar in self.data_provider.run_backtest_yield():
                logger.debug(f"Retrieved bar from DataProvider: {bar}")
                self.process_bar(bar)

        except Exception as e:
            logger.error(f"Error during backtest: {e}")
            raise

        end_time = time.time()
        logger.info(f"Backtest completed in {end_time - start_time:.2f} seconds.")
        self._print_results()

    def _print_results(self):
        """
        Outputs the results of the backtest and prints a performance report.
        """
        try:
            # Print performance report from PortfolioManager
            logger.info("Printing performance report...")
            self.portfolio.print_performance_report()
        except Exception as e:
            logger.error(f"Error printing results: {e}")
            raise


if __name__ == "__main__":
    runner = BacktestRunner(
        db_url="postgresql://postgres:your_secure_password@localhost:5432/oanda_data",
        table_name="natgas_data_cleaned",
        base_capital=10000000.0,
    )
    runner.run_backtest()