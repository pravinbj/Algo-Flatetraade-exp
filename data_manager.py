import os
import pandas as pd
from datetime import datetime

class DataManager:
    def __init__(self, base_dir="data"):
        """Initialize data manager with base directory for storing CSV files"""
        self.base_dir = base_dir
        self.ensure_directories()
        
    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.base_dir,
            os.path.join(self.base_dir, "market_data"),
            os.path.join(self.base_dir, "trades"),
            os.path.join(self.base_dir, "positions")
        ]
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)

    def get_market_data_filename(self, symbol, date=None):
        """Generate filename for market data CSV"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.base_dir, "market_data", f"{symbol}_{date}.csv")

    def get_trades_filename(self, date=None):
        """Generate filename for trades CSV"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.base_dir, "trades", f"trades_{date}.csv")

    def get_positions_filename(self, date=None):
        """Generate filename for positions CSV"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.base_dir, "positions", f"positions_{date}.csv")

    def save_market_data(self, symbol, data):
        """Save market data to CSV file"""
        filename = self.get_market_data_filename(symbol)
        df = pd.DataFrame([data])
        
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df.to_csv(filename, index=False)

    def save_trade(self, trade_data):
        """Save trade information to CSV file"""
        filename = self.get_trades_filename()
        df = pd.DataFrame([trade_data])
        
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df.to_csv(filename, index=False)

    def save_position(self, position_data):
        """Save position information to CSV file"""
        filename = self.get_positions_filename()
        df = pd.DataFrame([position_data])
        
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df.to_csv(filename, index=False)

    def load_market_data(self, symbol, date=None, last_n_records=None):
        """Load market data from CSV file"""
        filename = self.get_market_data_filename(symbol, date)
        if not os.path.exists(filename):
            return pd.DataFrame()
        
        df = pd.read_csv(filename)
        if last_n_records:
            return df.tail(last_n_records)
        return df

    def load_trades(self, date=None):
        """Load trades from CSV file"""
        filename = self.get_trades_filename(date)
        if not os.path.exists(filename):
            return pd.DataFrame()
        return pd.read_csv(filename)

    def load_positions(self, date=None):
        """Load positions from CSV file"""
        filename = self.get_positions_filename(date)
        if not os.path.exists(filename):
            return pd.DataFrame()
        return pd.read_csv(filename)
    


    
