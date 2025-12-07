"""
VWAP Strategy for Options Trading
Uses Flattrade API with proper authentication and data handling
"""
import time
import datetime
import pandas as pd
import numpy as np
from config import *
from auth import get_flattrade_token
from market_data import initialize_api
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_strategy.log'),
        logging.StreamHandler()
    ]
)

class VWAPStrategy:
    def __init__(self):
        self.api = None
        self.positions = {}
        self.daily_pnl = 0
        self.last_signal = None
        self.daily_trades = 0
        self.data_dir = os.path.join(os.getcwd(), 'data', 'market_data')
        os.makedirs(self.data_dir, exist_ok=True)
        
    def calculate_vwap(self, df):
        """Calculate VWAP from OHLCV data"""
        if df.empty:
            return df
            
        df = df.copy()
        if 'volume' not in df.columns:
            df['volume'] = 0
            
        # Calculate typical price * volume
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
        df['cumulative_volume'] = df['volume'].cumsum()
        df['cumulative_tpv'] = (df['typical_price'] * df['volume']).cumsum()
        
        # Calculate VWAP
        df['vwap'] = df['cumulative_tpv'] / df['cumulative_volume']
        df['vwap'] = df['vwap'].fillna(method='ffill')
        
        return df

    def get_atm_option_symbol(self, ltp, underlying, expiry):
        """Generate ATM option symbols based on current LTP"""
        if 'BANK' in underlying.upper():
            strike_step = BANKNIFTY_STRIKE_STEP
            base_symbol = 'BANKNIFTY'
        else:
            strike_step = NIFTY_STRIKE_STEP
            base_symbol = 'NIFTY'
            
        # Calculate ATM strike
        atm_strike = int(round(ltp / strike_step) * strike_step)
        
        call_symbol = f"{base_symbol}{expiry}C{atm_strike}"
        put_symbol = f"{base_symbol}{expiry}P{atm_strike}"
        
        return call_symbol, put_symbol, atm_strike

    def search_specific_contract(self, symbol):
        """Search for a specific contract symbol and return its token"""
        try:
            resp = self.api.searchscrip(exchange='NFO', searchtext=symbol)
            
            if not resp or resp.get('stat') != 'Ok':
                return None
                
            values = resp.get('values', [])
            for v in values:
                tsym = v.get('tsym', '')
                if tsym == symbol:
                    return v.get('token')
                    
            return None
        except Exception as e:
            logging.error(f"Error searching contract {symbol}: {e}")
            return None

    def initialize(self):
        """Initialize trading session"""
        try:
            # Initialize API using the market_data module
            from market_data import initialize_api
            self.api = initialize_api()
            
            if not self.api:
                logging.error("API initialization failed")
                return False
                
            logging.info(f"Strategy initialized for {SYMBOL}")
            logging.info(f"VWAP Window: {VWAP_WINDOW} candles")
            logging.info(f"Position Size: {QTY} contracts")
            
            return True
            
        except Exception as e:
            logging.error(f"Initialization error: {e}")
            return False

    def check_trading_hours(self):
        """Check if current time is within trading hours"""
        now = datetime.datetime.now().time()
        market_start = datetime.time(9, 15)  # 9:15 AM
        market_end = datetime.time(15, 30)   # 3:30 PM
        
        return market_start <= now <= market_end

    def save_market_data(self, symbol, data):
        """Save market data to CSV"""
        try:
            filename = f"{symbol}_market_data.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame([data])
            if os.path.exists(filepath):
                existing_df = pd.read_csv(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
                
            df.to_csv(filepath, index=False)
        except Exception as e:
            logging.error(f"Error saving market data: {e}")

    def save_trade(self, trade_data):
        """Save trade to CSV"""
        try:
            filename = "trades.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame([trade_data])
            if os.path.exists(filepath):
                existing_df = pd.read_csv(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
                
            df.to_csv(filepath, index=False)
        except Exception as e:
            logging.error(f"Error saving trade: {e}")

    def vwap_crossover_logic(self, df):
        """VWAP crossover signal logic"""
        if len(df) < 2:
            return None
            
        current = df.iloc[-1]
        previous = df.iloc[-2]
        
        # Buy signal when price crosses above VWAP
        if (previous['close'] <= previous['vwap'] and 
            current['close'] > current['vwap']):
            return "BUY_CALL"
            
        # Sell signal when price crosses below VWAP  
        elif (previous['close'] >= previous['vwap'] and 
              current['close'] < current['vwap']):
            return "BUY_PUT"
            
        return None

    def manage_position(self, position_id):
        """Manage open positions for SL/TP"""
        position = self.positions.get(position_id)
        if not position:
            return
            
        try:
            # Get current quote
            quote = self.api.get_quotes(exchange='NFO', token=position['token'])
            if not quote or quote.get('stat') != 'Ok':
                return
                
            current_price = float(quote.get('lp', 0))
            
            # Check stop loss
            if current_price <= position['stop_loss']:
                self.close_position(position_id, "SL Hit")
                return
                
            # Check take profit
            if current_price >= position['take_profit']:
                self.close_position(position_id, "TP Hit")
                return
                
        except Exception as e:
            logging.error(f"Error managing position: {e}")

    def close_position(self, position_id, reason=""):
        """Close an open position"""
        position = self.positions.get(position_id)
        if not position:
            return

        try:
            # Place sell order
            order_response = self.api.place_order(
                buy_or_sell='S',
                product_type='M',
                exchange='NFO',
                tradingsymbol=position['symbol'],
                quantity=position['qty'],
                price_type='MKT',
                price=0,
                trigger_price=None,
                retention='DAY'
            )
            
            if order_response and order_response.get('stat') == 'Ok':
                # Calculate PnL
                current_quote = self.api.get_quotes(exchange='NFO', token=position['token'])
                if current_quote and current_quote.get('stat') == 'Ok':
                    exit_price = float(current_quote.get('lp', 0))
                    pnl = (exit_price - position['entry_price']) * position['qty']
                    self.daily_pnl += pnl
                    
                    # Save trade record
                    trade_data = {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'symbol': position['symbol'],
                        'side': 'SELL',
                        'quantity': position['qty'],
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'pnl': pnl,
                        'reason': reason
                    }
                    self.save_trade(trade_data)
                    
                    logging.info(f"Position closed: {reason} | PnL: {pnl:.2f}")
                    del self.positions[position_id]
                    
        except Exception as e:
            logging.error(f"Error closing position: {e}")

    def fetch_ohlc_data(self, symbol, token, minutes=60):
        """Fetch OHLC data for the symbol"""
        try:
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(minutes=minutes)
            
            # Convert to timestamp
            start_ts = int(start_time.timestamp())
            end_ts = int(end_time.timestamp())
            
            data = self.api.get_time_price_series(
                exchange='NFO',
                token=token,
                starttime=start_ts,
                endtime=end_ts
            )
            
            if data and isinstance(data, list):
                df = pd.DataFrame(data)
                # Standardize column names
                column_map = {
                    'time': 'datetime',
                    'ssboe': 'datetime',
                    'into': 'open',
                    'inth': 'high', 
                    'intl': 'low',
                    'intc': 'close',
                    'intv': 'volume',
                    'v': 'volume',
                    'intoi': 'oi',
                    'oi': 'oi'
                }
                df.rename(columns=column_map, inplace=True)
                
                # Convert datetime if needed
                if 'datetime' in df.columns:
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    
                return df
                
        except Exception as e:
            logging.error(f"Error fetching OHLC data: {e}")
            
        return pd.DataFrame()

    def run(self):
        """Main strategy loop"""
        if not self.initialize():
            return

        # Get expiry (you might want to make this dynamic)
        if 'BANK' in SYMBOL.upper():
            expiry = BANKNIFTY_EXPIRY
        else:
            expiry = NIFTY_EXPIRY

        logging.info("Starting strategy loop...")
        
        while True:
            try:
                if not self.check_trading_hours():
                    logging.info("Outside trading hours. Waiting...")
                    time.sleep(300)  # 5 minutes
                    continue

                # Check daily loss limit
                if self.daily_pnl <= -MAX_DAILY_LOSS:
                    logging.info("Daily loss limit reached. Stopping trading.")
                    break

                # Get underlying LTP
                underlying_token = '26009' if 'BANK' in SYMBOL.upper() else '26000'
                quote_data = self.api.get_quotes(exchange='NSE', token=underlying_token)
                
                if not quote_data or quote_data.get('stat') != 'Ok':
                    logging.warning("Could not fetch underlying quote. Retrying...")
                    time.sleep(10)
                    continue
                    
                ltp = float(quote_data.get('lp', 0))
                
                # Save market data
                market_data = {
                    'timestamp': datetime.datetime.now().isoformat(),
                    'symbol': SYMBOL,
                    'ltp': ltp
                }
                self.save_market_data(SYMBOL, market_data)

                # Get ATM option symbols
                call_symbol, put_symbol, atm_strike = self.get_atm_option_symbol(ltp, SYMBOL, expiry)
                
                # For strategy, we'll use CALL options for this example
                option_token = self.search_specific_contract(call_symbol)
                if not option_token:
                    logging.warning(f"Could not find option contract: {call_symbol}")
                    time.sleep(10)
                    continue

                # Fetch OHLC data for the option
                df = self.fetch_ohlc_data(call_symbol, option_token, VWAP_WINDOW * 2)
                if df.empty:
                    logging.warning("No OHLC data available. Retrying...")
                    time.sleep(10)
                    continue

                # Calculate VWAP
                df = self.calculate_vwap(df)
                
                # Check for signals
                signal = self.vwap_crossover_logic(df)
                
                current_vwap = df['vwap'].iloc[-1] if not df.empty else 0
                current_close = df['close'].iloc[-1] if not df.empty else 0
                
                logging.info(f"Price: {current_close:.2f}, VWAP: {current_vwap:.2f}, Signal: {signal}")

                if signal and signal != self.last_signal:
                    # Close existing positions
                    for pos_id in list(self.positions.keys()):
                        self.close_position(pos_id, "Signal Change")

                    # Place new order
                    try:
                        option_quote = self.api.get_quotes(exchange='NFO', token=option_token)
                        if option_quote and option_quote.get('stat') == 'Ok':
                            entry_price = float(option_quote.get('lp', 0))
                            
                            order_response = self.api.place_order(
                                buy_or_sell='B',
                                product_type='M',
                                exchange='NFO',
                                tradingsymbol=call_symbol,
                                quantity=QTY,
                                price_type='MKT',
                                price=0,
                                trigger_price=None,
                                retention='DAY'
                            )
                            
                            if order_response and order_response.get('stat') == 'Ok':
                                order_id = order_response.get('norenordno')
                                position_id = f"{call_symbol}_{order_id}"
                                
                                self.positions[position_id] = {
                                    'symbol': call_symbol,
                                    'token': option_token,
                                    'qty': QTY,
                                    'entry_price': entry_price,
                                    'stop_loss': entry_price * (1 - MAX_LOSS_PER_TRADE/100),
                                    'take_profit': entry_price * (1 + TAKE_PROFIT_PERCENT/100)
                                }
                                
                                self.last_signal = signal
                                self.daily_trades += 1
                                
                                trade_data = {
                                    'timestamp': datetime.datetime.now().isoformat(),
                                    'symbol': call_symbol,
                                    'side': 'BUY',
                                    'quantity': QTY,
                                    'price': entry_price,
                                    'signal': signal
                                }
                                self.save_trade(trade_data)
                                
                                logging.info(f"New position opened: {call_symbol} at {entry_price:.2f}")
                                
                    except Exception as e:
                        logging.error(f"Error placing order: {e}")

                # Manage open positions
                for position_id in list(self.positions.keys()):
                    self.manage_position(position_id)

                # Status update
                logging.info(f"Daily PnL: {self.daily_pnl:.2f}, Open Positions: {len(self.positions)}")
                
                time.sleep(30)  # Wait 30 seconds between iterations
                
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                time.sleep(60)

if __name__ == "__main__":
    strategy = VWAPStrategy()
    strategy.run()