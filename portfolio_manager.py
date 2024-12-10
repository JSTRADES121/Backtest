import pandas as pd
from typing import Dict, Any
from events import order_filled_signal, stop_triggered_signal, OrderFilledEventData, StopTriggeredEventData
import logging
import numpy as np

logger = logging.getLogger("PortfolioManager")


class PortfolioManager:
    def __init__(self, starting_cash: float = 100000.0):
        """
        Initializes the PortfolioManager with starting cash and connects to order signals.

        :param starting_cash: The initial capital for the portfolio.
        """
        self.trades = []  # Each trade: {'instrument', 'action', 'size', 'filled_price', 'time', 'reason', 'net_profit', 'bars_held'}
        self.positions = {}  # {instrument: {"size": float, "entry_price": float, "entry_time": str}}
        self.cash = starting_cash
        self.starting_cash = starting_cash
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.market_data = {}  # {instrument: {"close": float}}
        self.equity_curve = [self.starting_cash]

        order_filled_signal.connect(self.on_order_filled)
        stop_triggered_signal.connect(self.on_stop_triggered)

    def on_order_filled(self, sender, order_data: OrderFilledEventData):
        """
        Handles an order-filled event. Updates positions, cash, and equity curve.
        """
        try:
            trade = order_data.order_data

            # Ensure required fields are present
            if 'time' not in trade:
                trade['time'] = pd.Timestamp.now().isoformat()
            if 'reason' not in trade:
                trade['reason'] = 'order_filled'

            # Initialize placeholders for net_profit and bars_held (computed when closing a position)
            trade['net_profit'] = 0.0
            trade['bars_held'] = 0

            self.update_positions(trade)
            self.update_cash(trade)
            self.update_equity_curve()
            self.trades.append(trade)
        except Exception as e:
            logger.error(f"Error handling filled order: {e}")

    def on_stop_triggered(self, sender, stop_info: StopTriggeredEventData):
        """
        Handles stop-triggered events by closing positions at the stop price if needed.
        """
        try:
            stop_details = stop_info.stop_info
            instrument = stop_details['instrument']
            reason = stop_details['reason']
            logger.debug(f"Stop triggered for {instrument}. Reason: {reason}")

            current_pos = self.positions.get(instrument, {})
            size = abs(current_pos.get('size', 0))
            if size > 0:
                # Determine action to close the position
                action = 'sell' if current_pos['size'] > 0 else 'buy'
                stop_trade = {
                    'instrument': instrument,
                    'action': action,
                    'size': size,
                    'filled_price': stop_details['stop_level'],
                    'time': pd.Timestamp.now().isoformat(),
                    'reason': reason,
                    'net_profit': 0.0,
                    'bars_held': 0
                }

                self.update_positions(stop_trade)
                self.update_cash(stop_trade)
                self.update_equity_curve()
                self.trades.append(stop_trade)
        except Exception as e:
            logger.error(f"Error handling stop trigger: {e}")

    def update_positions(self, trade: Dict[str, Any]):
        """
        Updates the portfolio positions based on the trade. 
        Computes net_profit and bars_held if a position is closed.
        Calls update_realized_pnl if position closes.
        """
        try:
            instr = trade['instrument']
            size_change = trade['size'] if trade['action'] == 'buy' else -trade['size']
            position = self.positions.get(instr, {'size': 0, 'entry_price': 0.0, 'entry_time': trade['time']})

            old_size = position['size']
            new_size = old_size + size_change

            if old_size == 0 and new_size != 0:
                # Opening a new position
                position['entry_price'] = trade['filled_price']
                position['entry_time'] = trade['time']
                position['size'] = new_size
                self.positions[instr] = position

            elif new_size == 0:
                # Closing the position
                entry_price = position['entry_price']
                entry_time = pd.Timestamp(position['entry_time'])
                exit_time = pd.Timestamp(trade['time'])

                # Calculate bars_held (if zero difference, at least 1)
                bars_held = max((exit_time - entry_time).days, 1)

                # Compute net profit based on whether we closed a long or short position
                if old_size > 0:  # Long position closed by selling
                    net_profit = (trade['filled_price'] - entry_price) * abs(size_change)
                else:  # Short position closed by buying
                    net_profit = (entry_price - trade['filled_price']) * abs(size_change)

                trade['net_profit'] = net_profit
                trade['bars_held'] = bars_held

                self.update_realized_pnl(trade)
                del self.positions[instr]

            else:
                # Adjusting (scaling in/out) the position
                total_cost_before = position['entry_price'] * position['size']
                trade_cost = trade['filled_price'] * size_change
                new_entry_price = (total_cost_before + trade_cost) / new_size
                position['entry_price'] = new_entry_price
                position['size'] = new_size
                # Keep original entry_time from the first opening trade
                self.positions[instr] = position

            logger.debug(f"Updated positions: {self.positions}")
        except Exception as e:
            logger.error(f"Error updating positions: {e}")

    def update_cash(self, trade: Dict[str, Any]):
        """
        Adjust the cash balance after a trade.
        """
        try:
            cost = trade['filled_price'] * trade['size']
            if trade['action'] == 'buy':
                self.cash -= cost
            else:  # sell
                self.cash += cost
            logger.debug(f"Updated cash: {self.cash}")
        except Exception as e:
            logger.error(f"Error updating cash: {e}")

    def update_realized_pnl(self, trade: Dict[str, Any]):
        """
        Updates realized PnL based on a fully closed position (trade with net_profit).
        """
        try:
            if 'net_profit' in trade:
                self.realized_pnl += trade['net_profit']
                logger.debug(f"Updated realized PnL: {self.realized_pnl}")
        except Exception as e:
            logger.error(f"Error updating realized PnL: {e}")

    def calculate_unrealized_pnl(self):
        """
        Calculates the unrealized PnL for all open positions using current market_data.
        """
        try:
            self.unrealized_pnl = 0.0
            for instr, position in self.positions.items():
                size = position['size']
                entry_price = position['entry_price']
                current_price = self.market_data.get(instr, {}).get('close', entry_price)
                if size > 0:  # long
                    self.unrealized_pnl += (current_price - entry_price) * size
                else:  # short
                    self.unrealized_pnl += (entry_price - current_price) * abs(size)
            logger.debug(f"Updated unrealized PnL: {self.unrealized_pnl}")
            return self.unrealized_pnl
        except Exception as e:
            logger.error(f"Error calculating unrealized PnL: {e}")
            return 0.0

    def get_pnl(self) -> float:
        """
        Returns total PnL (realized + unrealized).
        """
        try:
            total_pnl = self.realized_pnl + self.calculate_unrealized_pnl()
            logger.info(f"Total PnL: {total_pnl}")
            return total_pnl
        except Exception as e:
            logger.error(f"Error calculating total PnL: {e}")
            return 0.0

    def get_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns the current open positions.
        """
        return dict(self.positions)

    def get_trade_history(self) -> pd.DataFrame:
        """
        Returns the trade history as a pandas DataFrame.
        """
        try:
            trades_df = pd.DataFrame(self.trades)
            logger.debug(f"Trade history:\n{trades_df}")
            return trades_df
        except Exception as e:
            logger.error(f"Error fetching trade history: {e}")
            return pd.DataFrame()

    def update_equity_curve(self):
        """
        Updates the equity curve after each trade. Equity = cash + unrealized_pnl
        """
        try:
            total_equity = self.cash + self.calculate_unrealized_pnl()
            self.equity_curve.append(total_equity)
        except Exception as e:
            logger.error(f"Error updating equity curve: {e}")

    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """
        Computes a comprehensive performance summary. No hard-coded variables, all derived from trades and equity.
        """
        try:
            trades_df = self.get_trade_history()
            if trades_df.empty or 'net_profit' not in trades_df.columns:
                logger.warning("No trades or 'net_profit' field found. Returning empty metrics.")
                return {}

            # Separate long and short trades
            long_trades = trades_df[trades_df["action"] == "buy"]
            short_trades = trades_df[trades_df["action"] == "sell"]

            # Gross profit/loss
            all_gross_profit = trades_df[trades_df["net_profit"] > 0]["net_profit"].sum()
            all_gross_loss = trades_df[trades_df["net_profit"] < 0]["net_profit"].sum()

            long_gross_profit = long_trades[long_trades["net_profit"] > 0]["net_profit"].sum()
            long_gross_loss = long_trades[long_trades["net_profit"] < 0]["net_profit"].sum()

            short_gross_profit = short_trades[short_trades["net_profit"] > 0]["net_profit"].sum()
            short_gross_loss = short_trades[short_trades["net_profit"] < 0]["net_profit"].sum()

            # Net profit
            all_net_profit = all_gross_profit + all_gross_loss
            long_net_profit = long_gross_profit + long_gross_loss
            short_net_profit = short_gross_profit + short_gross_loss

            # Profit factors
            def pf(gp, gl):
                return gp / abs(gl) if gl != 0 else np.inf

            all_profit_factor = pf(all_gross_profit, all_gross_loss)
            long_profit_factor = pf(long_gross_profit, long_gross_loss)
            short_profit_factor = pf(short_gross_profit, short_gross_loss)

            # Number of trades
            total_trades = len(trades_df)
            long_trades_count = len(long_trades)
            short_trades_count = len(short_trades)

            # Winning/Losing trades
            all_winning_count = len(trades_df[trades_df["net_profit"] > 0])
            all_losing_count = len(trades_df[trades_df["net_profit"] < 0])

            long_winning_count = len(long_trades[long_trades["net_profit"] > 0])
            long_losing_count = len(long_trades[long_trades["net_profit"] < 0])

            short_winning_count = len(short_trades[short_trades["net_profit"] > 0])
            short_losing_count = len(short_trades[short_trades["net_profit"] < 0])

            def pct(part, whole):
                return (part / whole) * 100 if whole > 0 else 0

            all_winning_percentage = pct(all_winning_count, total_trades)
            all_losing_percentage = 100 - all_winning_percentage

            long_winning_percentage = pct(long_winning_count, long_trades_count)
            long_losing_percentage = 100 - long_winning_percentage

            short_winning_percentage = pct(short_winning_count, short_trades_count)
            short_losing_percentage = 100 - short_winning_percentage

            # Average trade net profit
            all_avg_trade = all_net_profit / total_trades if total_trades > 0 else 0
            long_avg_trade = long_net_profit / long_trades_count if long_trades_count > 0 else 0
            short_avg_trade = short_net_profit / short_trades_count if short_trades_count > 0 else 0

            # Largest wins/losses
            largest_win_all = trades_df["net_profit"].max() if not trades_df.empty else 0
            largest_loss_all = trades_df["net_profit"].min() if not trades_df.empty else 0

            largest_win_long = long_trades["net_profit"].max() if not long_trades.empty else 0
            largest_loss_long = long_trades["net_profit"].min() if not long_trades.empty else 0

            largest_win_short = short_trades["net_profit"].max() if not short_trades.empty else 0
            largest_loss_short = short_trades["net_profit"].min() if not short_trades.empty else 0

            # Max consecutive wins/losses (All)
            profit_series = trades_df["net_profit"].values
            max_consec_wins = 0
            max_consec_losses = 0
            current_wins = 0
            current_losses = 0
            for p in profit_series:
                if p > 0:
                    current_wins += 1
                    max_consec_wins = max(max_consec_wins, current_wins)
                    current_losses = 0
                else:
                    current_losses += 1
                    max_consec_losses = max(max_consec_losses, current_losses)
                    current_wins = 0

            # Average bars in winning/losing trades
            if 'bars_held' in trades_df.columns:
                winning_bars = trades_df[trades_df["net_profit"] > 0]["bars_held"]
                losing_bars = trades_df[trades_df["net_profit"] < 0]["bars_held"]
                avg_bars_win = winning_bars.mean() if not winning_bars.empty else 0
                avg_bars_loss = losing_bars.mean() if not losing_bars.empty else 0
            else:
                avg_bars_win = 0
                avg_bars_loss = 0

            # Max Drawdown & Max Equity Run-up
            equity_array = np.array(self.equity_curve)
            running_max = np.maximum.accumulate(equity_array)
            drawdowns = (equity_array - running_max)
            max_drawdown = drawdowns.min()  # negative number
            max_equity_runup = (running_max.max() - self.starting_cash)

            # Calculate total_days for annualized return
            if not trades_df.empty and 'time' in trades_df.columns:
                trades_df["time"] = pd.to_datetime(trades_df["time"])
                total_days = (trades_df["time"].max() - trades_df["time"].min()).days
                if total_days < 1:
                    total_days = 1
            else:
                total_days = 1

            final_equity = self.equity_curve[-1]
            return_on_initial = ((final_equity - self.starting_cash) / self.starting_cash) * 100
            annual_rate_return = ((final_equity / self.starting_cash) ** (365 / total_days) - 1) * 100 if total_days > 0 else 0

            # Return Retracement Ratio
            return_retracement_ratio = (all_net_profit / abs(max_drawdown)) if max_drawdown < 0 else np.inf

            # RINA Index (no standard formula given, so using a ratio similar to return_retracement_ratio weighted by win%)
            rina_index = (all_net_profit * (all_winning_percentage / 100)) / abs(max_drawdown) if max_drawdown < 0 else np.inf

            metrics = {
                "Total Net Profit (All Trades)": all_net_profit,
                "Total Net Profit (Long Trades)": long_net_profit,
                "Total Net Profit (Short Trades)": short_net_profit,

                "Gross Profit (All Trades)": all_gross_profit,
                "Gross Profit (Long Trades)": long_gross_profit,
                "Gross Profit (Short Trades)": short_gross_profit,

                "Gross Loss (All Trades)": all_gross_loss,
                "Gross Loss (Long Trades)": long_gross_loss,
                "Gross Loss (Short Trades)": short_gross_loss,

                "Profit Factor (All Trades)": all_profit_factor,
                "Profit Factor (Long Trades)": long_profit_factor,
                "Profit Factor (Short Trades)": short_profit_factor,

                "Number of Trades (All)": total_trades,
                "Number of Trades (Long)": long_trades_count,
                "Number of Trades (Short)": short_trades_count,

                "Winning Percentage (All Trades)": all_winning_percentage,
                "Winning Percentage (Long Trades)": long_winning_percentage,
                "Winning Percentage (Short Trades)": short_winning_percentage,

                "Losing Percentage (All Trades)": all_losing_percentage,
                "Losing Percentage (Long Trades)": long_losing_percentage,
                "Losing Percentage (Short Trades)": short_losing_percentage,

                "Average Trade Net Profit (All)": all_avg_trade,
                "Average Trade Net Profit (Long)": long_avg_trade,
                "Average Trade Net Profit (Short)": short_avg_trade,

                "Largest Winning Trade (All)": largest_win_all,
                "Largest Winning Trade (Long)": largest_win_long,
                "Largest Winning Trade (Short)": largest_win_short,

                "Largest Losing Trade (All)": largest_loss_all,
                "Largest Losing Trade (Long)": largest_loss_long,
                "Largest Losing Trade (Short)": largest_loss_short,

                "Max Consecutive Winning Trades (All)": max_consec_wins,
                "Max Consecutive Losing Trades (All)": max_consec_losses,

                "Average Bars in Winning Trades (All)": avg_bars_win,
                "Average Bars in Losing Trades (All)": avg_bars_loss,

                "Max Drawdown (All Trades)": max_drawdown,
                "Return on Initial Capital": return_on_initial,
                "Annual Rate of Return": annual_rate_return,
                "Return Retracement Ratio": return_retracement_ratio,
                "RINA Index": rina_index,
                "Max Equity Run-up": max_equity_runup
            }

            return metrics
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {}

    def print_performance_report(self):
        """
        Prints a detailed performance report.
        """
        metrics = self.calculate_performance_metrics()
        print("\n=== PERFORMANCE REPORT ===")
        for key, value in metrics.items():
            print(f"{key}: {value}")