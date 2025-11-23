from NorenRestApiPy.NorenApi import NorenApi
import logging
import datetime
import pandas as pd
from config import *

class FlatTradeNorenAPI(NorenApi):
    def __init__(self):
        """Initialize FlatTrade API with Noren API"""
        super().__init__(host=API_URL)
        self.token = None
        self.login_status = False

    def login(self):
        """Login to FlatTrade using NorenAPI"""
        try:
            # Get current time for api_time
            api_time = datetime.datetime.now().strftime('%H:%M:%S')
            
            # Login using the NorenAPI
            ret = super().login(
                userid=CLIENT_ID,
                password=API_SECRET,  # Your login password
                twoFA=TOTP_SECRET,   # TOTP or OTP
                vendor_code=VENDOR_CODE,
                api_secret=API_KEY,
                imei=IMEI
            )
            
            if ret != None:
                self.login_status = True
                logging.info("Login successful ✅")
                return True
            else:
                logging.error("Login failed ❌")
                return False
                
        except Exception as e:
            logging.error(f"Login error: {str(e)}")
            raise

    def get_ltp(self, symbol):
        """Get last traded price"""
        try:
            ret = self.get_quotes(exchange='NSE', token=symbol)
            if ret != None and 'lp' in ret:
                return float(ret['lp'])
            return None
        except Exception as e:
            logging.error(f"Error getting LTP for {symbol}: {str(e)}")
            raise

    def fetch_ohlc(self, symbol, interval="1", from_date=None, to_date=None):
        """
        Fetch historical data
        interval options: "1", "3", "5", "10", "15", "30", "60", "D"
        """
        try:
            if from_date is None:
                from_date = datetime.datetime.now().strftime('%Y-%m-%d')
            if to_date is None:
                to_date = from_date

            ret = self.get_time_price_series(
                exchange='NSE',
                token=symbol,
                starttime=from_date,
                endtime=to_date,
                interval=interval
            )
            
            if ret is None:
                return pd.DataFrame()
                
            df = pd.DataFrame(ret)
            if not df.empty:
                df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi']
                df['datetime'] = pd.to_datetime(df['datetime'])
                for col in ['open', 'high', 'low', 'close', 'volume', 'oi']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
            
        except Exception as e:
            logging.error(f"Error fetching OHLC for {symbol}: {str(e)}")
            raise

    def place_order(self, symbol, buy_or_sell, qty, price=0, order_type="MKT"):
        """
        Place an order
        buy_or_sell: 'B' for buy, 'S' for sell
        order_type: 'MKT' for Market, 'LMT' for Limit
        """
        try:
            order_params = {
                'buy_or_sell': 'B' if buy_or_sell == "BUY" else 'S',
                'exchange': 'NSE',
                'tradingsymbol': symbol,
                'quantity': qty,
                'discloseqty': 0,
                'price_type': order_type,
                'price': price if order_type == 'LMT' else 0,
                'trigger_price': None,
                'retention': 'DAY',
                'remarks': 'VWAP strategy order'
            }
            
            ret = self.place_order(**order_params)
            
            if ret != None and 'norenordno' in ret:
                logging.info(f"Order placed successfully: {ret['norenordno']}")
                return ret['norenordno']
            else:
                logging.error("Order placement failed")
                return None
                
        except Exception as e:
            logging.error(f"Error placing order: {str(e)}")
            raise

    def modify_order(self, order_no, price=None, quantity=None, trigger_price=None):
        """Modify an existing order"""
        try:
            ret = self.modify_order(
                orderno=order_no,
                exchange='NSE',
                newquantity=quantity,
                newprice_type='LMT' if price else 'MKT',
                newprice=price if price else 0,
                newtrigger_price=trigger_price
            )
            
            if ret != None and 'result' in ret:
                logging.info(f"Order modified successfully: {order_no}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Error modifying order: {str(e)}")
            raise

    def cancel_order(self, order_no):
        """Cancel an order"""
        try:
            ret = self.cancel_order(orderno=order_no)
            if ret != None and 'result' in ret:
                logging.info(f"Order cancelled successfully: {order_no}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Error cancelling order: {str(e)}")
            raise

    def get_order_status(self, order_no):
        """Get the current status of an order"""
        try:
            ret = self.single_order_history(orderno=order_no)
            if ret != None and len(ret) > 0:
                return ret[0]['status']
            return None
            
        except Exception as e:
            logging.error(f"Error getting order status: {str(e)}")
            raise

    def get_positions(self):
        """Get current positions"""
        try:
            ret = self.get_positions()
            if ret != None:
                return pd.DataFrame(ret)
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error getting positions: {str(e)}")
            raise