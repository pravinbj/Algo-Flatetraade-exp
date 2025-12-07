"""
Live Trading Dashboard - All Instruments
"""

import time, datetime, pandas as pd, numpy as np, os, threading, warnings, logging
from config import *
from market_data import initialize_api

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    SL_PERCENT = 0; TP_PERCENT = 0.60; TRAILING_SL_PERCENT = 0.30; EMA_PERIOD = 5
    ENTRY_DELAY = 30; BACKFILL_DAYS = 3; DATA_DIR = "data/market_data"
    LOT_NIFTY = 75; LOT_BANKNIFTY = 25; COMMISSION = 0.0003
    MAX_DAILY_LOSS = 5000; DASH_REFRESH = 2
    STRIKES_COUNT = 1

class TradingStrategy:
    def __init__(self):
        self.api = None; self.positions = {}; self.pnl = 0; self.trades = 0
        self.last_signal = None; self.running = True; self.data_cache = {}
        self.instruments = []; self.instrument_data = {}; self.closed_trades = []
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        self.position_counter = 1; self.trade_counter = 1

    def calculate_vwap(self, df):
        if df.empty or len(df) < 2: return df
        df = df.copy()
        if "volume" not in df.columns: df["volume"] = 1
        
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["date"] = pd.to_datetime(df["datetime"]).dt.date
        df.loc[df["volume"] == 0, "volume"] = 1
        
        df["tpv"] = df["typical_price"] * df["volume"]
        df["cum_vol"] = df.groupby("date")["volume"].cumsum()
        df["cum_tpv"] = df.groupby("date")["tpv"].cumsum()
        
        df["vwap"] = df["cum_tpv"] / df["cum_vol"]
        df["vwap"] = df["vwap"].ffill().bfill()
        
        df.drop(["tpv", "cum_vol", "cum_tpv", "date", "typical_price"], 
                axis=1, inplace=True, errors="ignore")
        return df

    def calculate_ema(self, df):
        df = df.copy()
        df["ema"] = df["close"].ewm(span=Config.EMA_PERIOD, adjust=False).mean()
        df["ema"] = df["ema"].bfill()
        return df

    def detect_signal(self, df):
        if len(df) < 2: return None, None
        prev, curr = df.iloc[-2], df.iloc[-1]
        
        if (prev["ema"] <= prev["vwap"] and
            curr["ema"] > curr["vwap"] and
            curr["close"] > curr["vwap"] and
            curr["close"] > curr["ema"]):
            return "BUY_CALL", curr["close"]
        
        elif (prev["ema"] >= prev["vwap"] and
              curr["ema"] < curr["vwap"] and
              curr["close"] < curr["vwap"] and
              curr["close"] < curr["ema"]):
            return "BUY_PUT", curr["close"]
        
        return None, None

    def get_atm_strikes(self, ltp, underlying, expiry):
        if 'BANK' in underlying.upper():
            strike_step = BANKNIFTY_STRIKE_STEP
            base = 'BANKNIFTY'
            lot = Config.LOT_BANKNIFTY
        else:
            strike_step = NIFTY_STRIKE_STEP
            base = 'NIFTY'
            lot = Config.LOT_NIFTY
        
        atm_strike = int(round(ltp / strike_step) * strike_step)
        
        strikes = []
        for i in range(-Config.STRIKES_COUNT, Config.STRIKES_COUNT + 1):
            strike = atm_strike + (i * strike_step)
            strikes.append(strike)
        
        return strikes, base, lot

    def search_contract(self, symbol):
        try:
            resp = self.api.searchscrip(exchange='NFO', searchtext=symbol)
            if resp and resp.get('stat') == 'Ok':
                for v in resp.get('values', []):
                    if v.get('tsym', '').upper() == symbol.upper():
                        return v.get('token')
        except: pass
        return None

    def get_quote(self, symbol):
        token = self.search_contract(symbol)
        if token:
            return self.api.get_quotes(exchange='NFO', token=token)
        return None

    def place_order(self, symbol, qty, action='B'):
        try:
            token = self.search_contract(symbol)
            if not token: return None
            
            order = self.api.place_order(
                buy_or_sell=action, product_type='M', exchange='NFO',
                tradingsymbol=symbol, quantity=qty, price_type='MKT', price=0
            )
            
            if order and order.get('stat') == 'Ok':
                order_id = order.get('norenordno')
                logging.info(f"Order {action} placed: {symbol} | Qty: {qty} | ID: {order_id}")
                return order_id
        except: pass
        return None

    def enter_trade(self, signal, symbol, price, lot):
        print(f"\n[ENTRY] Signal: {signal} | Symbol: {symbol} | Price: {price}")
        print(f"Waiting {Config.ENTRY_DELAY}s for confirmation...")
        
        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).seconds < Config.ENTRY_DELAY:
            quote = self.get_quote(symbol)
            if quote:
                curr_price = float(quote.get('lp', price))
                if symbol in self.instrument_data:
                    self.instrument_data[symbol]['close'] = curr_price
                    self.instrument_data[symbol]['ltp'] = curr_price
                remaining = Config.ENTRY_DELAY - (datetime.datetime.now() - start).seconds
                print(f"  Current: {curr_price:.2f} | Time remaining: {remaining}s", end='\r')
            time.sleep(1)
        
        order_id = self.place_order(symbol, lot, 'B')
        if order_id:
            pos_id = f"{symbol}_{order_id}"
            self.positions[pos_id] = {
                'trade_no': self.trade_counter,
                'symbol': symbol,
                'type': 'CALL' if signal == 'BUY_CALL' else 'PUT',
                'entry_time': datetime.datetime.now(),
                'entry': price,
                'qty': lot,
                'order_id': order_id,
                'sl': price * (1 - Config.SL_PERCENT) if Config.SL_PERCENT > 0 else price * 0.5,
                'tp': price * (1 + Config.TP_PERCENT),
                'high': price,
                'low': price,
                'current_sl': price * (1 - Config.SL_PERCENT) if Config.SL_PERCENT > 0 else price * 0.5,
                'max_mtm': 0,
                'min_mtm': 0,
                'exit_reason': None,
                'exit_price': None,
                'pnl': 0,
                'status': 'OPEN'
            }
            self.trade_counter += 1
            print(f"\n[TRADE ENTERED] {symbol} @ {price}")
            return pos_id
        return None

    def manage_positions(self):
        for pos_id, pos in list(self.positions.items()):
            quote = self.get_quote(pos['symbol'])
            if not quote or quote.get('stat') != 'Ok': continue
            
            curr = float(quote.get('lp', 0))
            high = float(quote.get('high', curr))
            low = float(quote.get('low', curr))
            
            pos['high'] = max(pos['high'], high)
            pos['low'] = min(pos['low'], low)
            pos['max_mtm'] = max(pos['max_mtm'], (pos['high'] - pos['entry']) * pos['qty'])
            pos['min_mtm'] = min(pos['min_mtm'], (pos['low'] - pos['entry']) * pos['qty'])
            
            if Config.TRAILING_SL_PERCENT > 0 and Config.SL_PERCENT > 0:
                trail_sl = pos['high'] * (1 - Config.TRAILING_SL_PERCENT)
                pos['current_sl'] = max(pos['current_sl'], trail_sl)
            
            exit_price = None; reason = None; status = None
            if Config.SL_PERCENT > 0 and low <= pos['current_sl']:
                exit_price = pos['current_sl']; reason = "SL"; status = "SL_HIT"
            elif high >= pos['tp']:
                exit_price = pos['tp']; reason = "TP"; status = "TGT_HIT"
            elif not self.in_trading_hours():
                exit_price = curr; reason = "EOD"; status = "EOD_CLOSE"
            
            if reason:
                self.close_position(pos_id, reason, exit_price, status)

    def close_position(self, pos_id, reason, price=None, status=None):
        pos = self.positions.get(pos_id)
        if not pos: return
        
        try:
            if price is None:
                quote = self.get_quote(pos['symbol'])
                price = float(quote.get('lp', pos['entry'])) if quote else pos['entry']
            
            self.place_order(pos['symbol'], pos['qty'], 'S')
            
            pnl = (price - pos['entry']) * pos['qty']
            pnl -= (pos['entry'] + price) * pos['qty'] * Config.COMMISSION
            self.pnl += pnl
            
            trade_record = {
                'trade_no': pos['trade_no'],
                'symbol': pos['symbol'],
                'type': pos['type'],
                'entry': pos['entry'],
                'entry_time': pos['entry_time'],
                'exit': price,
                'exit_time': datetime.datetime.now(),
                'qty': pos['qty'],
                'pnl': pnl,
                'mtm': (price - pos['entry']) * pos['qty'],
                'max_mtm': pos['max_mtm'],
                'min_mtm': pos['min_mtm'],
                'reason': reason,
                'status': status or 'CLOSED'
            }
            
            self.closed_trades.append(trade_record)
            if len(self.closed_trades) > 20:
                self.closed_trades = self.closed_trades[-20:]
            
            print(f"\n[EXIT] {pos['symbol']} | Reason: {reason} | PnL: {pnl:.2f}")
            del self.positions[pos_id]
        except Exception as e:
            logging.error(f"Error closing position: {e}")

    def manual_exit(self):
        if not self.positions:
            print("\nNo open positions to exit")
            return
        
        print("\nOpen Positions:")
        for pos_id, pos in self.positions.items():
            quote = self.get_quote(pos['symbol'])
            curr = float(quote.get('lp', pos['entry'])) if quote else pos['entry']
            mtm = (curr - pos['entry']) * pos['qty']
            print(f"  {pos['trade_no']}. {pos['symbol']} | Entry: {pos['entry']:.2f} | Current: {curr:.2f} | MTM: {mtm:.2f}")
        
        try:
            choice = input("\nEnter trade number to exit (0 to cancel): ")
            if choice == '0': return
            
            for pos_id, pos in self.positions.items():
                if str(pos['trade_no']) == choice:
                    self.close_position(pos_id, "MANUAL", None, "MANUAL_CLOSE")
                    return
            print("Invalid trade number")
        except: pass

    def initialize_instruments(self):
        self.instruments = []
        
        nifty_token = '26000'
        banknifty_token = '26009'
        
        nifty_quote = self.api.get_quotes(exchange='NSE', token=nifty_token)
        banknifty_quote = self.api.get_quotes(exchange='NSE', token=banknifty_token)
        
        if nifty_quote and nifty_quote.get('stat') == 'Ok':
            nifty_ltp = float(nifty_quote.get('lp', 0))
            nifty_strikes, nifty_base, _ = self.get_atm_strikes(
                nifty_ltp, 'NIFTY', NIFTY_EXPIRY
            )
            
            for strike in nifty_strikes:
                for opt_type in ['C', 'P']:
                    symbol = f"{nifty_base}{NIFTY_EXPIRY}{opt_type}{strike}"
                    self.instruments.append({
                        'symbol': symbol,
                        'underlying': 'NIFTY',
                        'strike': strike,
                        'type': opt_type,
                        'lot_size': Config.LOT_NIFTY
                    })
        
        if banknifty_quote and banknifty_quote.get('stat') == 'Ok':
            banknifty_ltp = float(banknifty_quote.get('lp', 0))
            banknifty_strikes, banknifty_base, _ = self.get_atm_strikes(
                banknifty_ltp, 'BANKNIFTY', BANKNIFTY_EXPIRY
            )
            
            for strike in banknifty_strikes:
                for opt_type in ['C', 'P']:
                    symbol = f"{banknifty_base}{BANKNIFTY_EXPIRY}{opt_type}{strike}"
                    self.instruments.append({
                        'symbol': symbol,
                        'underlying': 'BANKNIFTY',
                        'strike': strike,
                        'type': opt_type,
                        'lot_size': Config.LOT_BANKNIFTY
                    })
        
        logging.info(f"Initialized {len(self.instruments)} instruments")

    def update_instrument_data(self, symbol, quote):
        if not quote or quote.get('stat') != 'Ok': return
        
        try:
            curr = float(quote.get('lp', 0))
            open_price = float(quote.get('o', curr))
            high = float(quote.get('h', curr))
            low = float(quote.get('l', curr))
            volume = int(quote.get('v', 0))
            
            data_point = {
                'datetime': datetime.datetime.now(),
                'open': open_price,
                'high': high,
                'low': low,
                'close': curr,
                'volume': volume
            }
            
            filepath = os.path.join(Config.DATA_DIR, f"{symbol}.csv")
            new_row = pd.DataFrame([data_point])
            
            if os.path.exists(filepath):
                existing = pd.read_csv(filepath)
                existing['datetime'] = pd.to_datetime(existing['datetime'])
                new_row['datetime'] = pd.to_datetime(new_row['datetime'])
                updated = pd.concat([existing, new_row], ignore_index=True)
                updated = updated.drop_duplicates(subset=['datetime'], keep='last')
                updated.to_csv(filepath, index=False)
            else:
                new_row.to_csv(filepath, index=False)
            
            df = pd.read_csv(filepath)
            self.data_cache[symbol] = df
            
            if len(df) >= Config.EMA_PERIOD:
                df = self.calculate_ema(self.calculate_vwap(df))
                self.data_cache[symbol] = df
                
                self.instrument_data[symbol] = {
                    'symbol': symbol,
                    'ltp': curr,
                    'vwap': df.iloc[-1]['vwap'] if 'vwap' in df.columns else 0,
                    'ema': df.iloc[-1]['ema'] if 'ema' in df.columns else 0,
                    'signal': None,
                    'volume': volume,
                    'open': open_price,
                    'high': high,
                    'low': low
                }
                
                signal, _ = self.detect_signal(df)
                if signal:
                    self.instrument_data[symbol]['signal'] = signal
                    
        except Exception as e:
            logging.error(f"Error updating {symbol}: {e}")

    def display_dashboard(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print("="*120)
        print("LIVE TRADING DASHBOARD")
        print("="*50)
        print(f"Date: {datetime.datetime.now().strftime('%d-%m-%Y')}")
        print(f"Time: {datetime.datetime.now().strftime('%H:%M:%S')}")
        print(f"Daily P&L: {self.pnl:.2f} | Open Trades: {len(self.positions)} | Total Instruments: {len(self.instruments)}")
        print("="*120)
        
        # ALL INSTRUMENTS - LIVE DATA
        print("\nALL INSTRUMENTS - LIVE DATA:")
        print("-"*120)
        print(f"{'Symbol':<20} {'LTP':<8} {'VWAP':<8} {'EMA':<8} {'Signal':<10} {'Volume':<10}")
        print("-"*120)
        
        sorted_instruments = sorted(self.instrument_data.values(), key=lambda x: x['symbol'])
        
        for inst in sorted_instruments:
            signal_display = inst.get('signal', '')
            
            if signal_display == 'BUY_CALL':
                signal_display = "\033[92mBUY_CALL\033[0m"
            elif signal_display == 'BUY_PUT':
                signal_display = "\033[91mBUY_PUT\033[0m"
            
            print(f"{inst['symbol']:<20} {inst['ltp']:<8.2f} {inst['vwap']:<8.2f} "
                  f"{inst['ema']:<8.2f} {signal_display:<10} {inst['volume']:<10}")
        
        # OPEN POSITIONS
        print("\n\nOPEN POSITIONS:")
        print("-"*120)
        if self.positions:
            print(f"{'#':<3} {'Symbol':<20} {'Type':<6} {'Entry':<8} {'Entry Time':<10} {'LTP':<8} {'Qty':<6} {'MTM':<8} {'VWAP':<8} {'EMA':<8} {'Signal':<10} {'Status':<12}")
            print("-"*120)
            for pos_id, pos in self.positions.items():
                quote = self.get_quote(pos['symbol'])
                curr = float(quote.get('lp', pos['entry'])) if quote else pos['entry']
                mtm = (curr - pos['entry']) * pos['qty']
                mtm -= (pos['entry'] + curr) * pos['qty'] * Config.COMMISSION
                entry_time = pos['entry_time'].strftime('%H:%M:%S') if hasattr(pos['entry_time'], 'strftime') else str(pos['entry_time'])
                
                # Get current instrument data for VWAP, EMA, Signal
                inst_data = self.instrument_data.get(pos['symbol'], {})
                vwap = inst_data.get('vwap', 0)
                ema = inst_data.get('ema', 0)
                signal = inst_data.get('signal', '')
                
                if signal == 'BUY_CALL':
                    signal = "\033[92mBUY_CALL\033[0m"
                elif signal == 'BUY_PUT':
                    signal = "\033[91mBUY_PUT\033[0m"
                
                print(f"{pos['trade_no']:<3} {pos['symbol']:<20} {pos['type']:<6} "
                      f"{pos['entry']:<8.2f} {entry_time:<10} {curr:<8.2f} {pos['qty']:<6} "
                      f"{mtm:<8.2f} {vwap:<8.2f} {ema:<8.2f} {signal:<10} {'OPEN':<12}")
        else:
            print("No open positions")
        
        # RECENTLY CLOSED TRADES (last 10)
        if self.closed_trades:
            print("\n\nRECENTLY CLOSED TRADES:")
            print("-"*120)
            print(f"{'#':<3} {'Symbol':<20} {'Type':<6} {'Entry':<8} {'Exit':<8} {'Entry Time':<10} {'Exit Time':<10} {'Qty':<6} {'PnL':<8} {'MTM':<8} {'Status':<12}")
            print("-"*120)
            
            for trade in self.closed_trades[-10:]:
                entry_time = trade['entry_time'].strftime('%H:%M:%S') if hasattr(trade['entry_time'], 'strftime') else str(trade['entry_time'])
                exit_time = trade['exit_time'].strftime('%H:%M:%S') if hasattr(trade['exit_time'], 'strftime') else str(trade['exit_time'])
                
                status = trade['status']
                if status == 'SL_HIT':
                    status = "\033[91mSL_HIT\033[0m"
                elif status == 'TGT_HIT':
                    status = "\033[92mTGT_HIT\033[0m"
                elif status == 'EOD_CLOSE':
                    status = "\033[93mEOD_CLOSE\033[0m"
                elif status == 'MANUAL_CLOSE':
                    status = "\033[94mMANUAL_CLOSE\033[0m"
                
                pnl_color = ""
                if trade['pnl'] > 0:
                    pnl_color = "\033[92m"
                elif trade['pnl'] < 0:
                    pnl_color = "\033[91m"
                
                print(f"{trade['trade_no']:<3} {trade['symbol']:<20} {trade['type']:<6} "
                      f"{trade['entry']:<8.2f} {trade['exit']:<8.2f} {entry_time:<10} {exit_time:<10} "
                      f"{trade['qty']:<6} {pnl_color}{trade['pnl']:<8.2f}\033[0m {trade['mtm']:<8.2f} {status:<12}")
        
        print("\n" + "="*120)
        print("Commands: (M) Manual Exit | (Q) Quit")
        print("="*120)

    def update_dashboard_thread(self):
        while self.running:
            try:
                for inst in self.instruments:
                    symbol = inst['symbol']
                    quote = self.get_quote(symbol)
                    if quote:
                        self.update_instrument_data(symbol, quote)
                
                self.display_dashboard()
                time.sleep(Config.DASH_REFRESH)
            except Exception as e:
                logging.error(f"Dashboard error: {e}")
                time.sleep(1)

    def in_trading_hours(self):
        now = datetime.datetime.now().time()
        return datetime.time(9, 15) <= now <= datetime.time(15, 30)

    def initialize(self):
        try:
            self.api = initialize_api()
            if not self.api: return False
            
            self.initialize_instruments()
            logging.info("Trading initialized")
            return True
        except Exception as e:
            logging.error(f"Initialization error: {e}")
            return False

    def run(self):
        if not self.initialize(): return
        
        import msvcrt
        dashboard_thread = threading.Thread(target=self.update_dashboard_thread, daemon=True)
        dashboard_thread.start()
        
        print("\nStarting live trading... Press M for manual exit, Q to quit")
        
        try:
            while self.running:
                if not self.in_trading_hours():
                    time.sleep(60)
                    continue
                
                if self.pnl <= -Config.MAX_DAILY_LOSS:
                    print("\nDaily loss limit reached!")
                    break
                
                if msvcrt.kbhit():
                    key = msvcrt.getch().decode().lower()
                    if key == 'q':
                        print("\nExiting...")
                        break
                    elif key == 'm':
                        self.manual_exit()
                
                self.manage_positions()
                
                for inst in self.instruments:
                    symbol = inst['symbol']
                    if symbol in self.data_cache and len(self.data_cache[symbol]) >= Config.EMA_PERIOD:
                        df = self.data_cache[symbol]
                        signal, sig_price = self.detect_signal(df)
                        
                        if signal:
                            has_position = any(pos['symbol'] == symbol for pos in self.positions.values())
                            
                            if not has_position and signal != self.last_signal:
                                pos_id = self.enter_trade(signal, symbol, sig_price, inst['lot_size'])
                                if pos_id:
                                    self.last_signal = signal
                                    self.trades += 1
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\nStopped by user")
        except Exception as e:
            logging.error(f"Main loop error: {e}")
        finally:
            self.running = False
            print("\nTrading stopped")

if __name__ == "__main__":
    print("\n" + "="*120)
    print("LIVE TRADING DASHBOARD")
    print("="*120)
    print(f"Tracking: NIFTY ±{Config.STRIKES_COUNT} strikes & BANKNIFTY ±{Config.STRIKES_COUNT} strikes")
    print(f"NIFTY Expiry: {NIFTY_EXPIRY}")
    print(f"BANKNIFTY Expiry: {BANKNIFTY_EXPIRY}")
    print("="*120)
    
    strategy = TradingStrategy()
    strategy.run()