from market_data import FlatTradeAPI, initialize_api
import datetime
import time
import math
import pandas as pd

def is_market_open():
    """Check if market is currently open"""
    now = datetime.datetime.now()
    market_start = now.replace(hour=9, minute=15, second=0)
    market_end = now.replace(hour=15, minute=30, second=0)
    return market_start <= now <= market_end

def get_nearest_strike(ltp):
    """Get the nearest strike price for Nifty options"""
    return round(ltp / 50) * 50

def get_expiry():
    """Get current week's expiry date"""
    today = datetime.datetime.now()
    days_to_thursday = (3 - today.weekday()) % 7
    expiry = today + datetime.timedelta(days=days_to_thursday)
    return expiry.strftime("%d%b%y").upper()

def get_option_tokens(api, index_token='26000'):
    """Get ATM Call and Put option tokens for Nifty"""
    try:
        # Get Nifty spot price
        nifty_quote = api.get_quotes(exchange='NSE', token=index_token)
        if not nifty_quote:
            raise Exception("Unable to fetch Nifty spot price")

        spot_price = float(nifty_quote.get('lp', 0))
        atm_strike = get_nearest_strike(spot_price)
        expiry = get_expiry()

        # Construct option symbols
        call_symbol = f"NIFTY{expiry}{atm_strike}CE"
        put_symbol = f"NIFTY{expiry}{atm_strike}PE"

        # Get tokens for these symbols
        search_resp = api.searchscrip(exchange='NFO', searchtext='NIFTY')
        if not search_resp:
            raise Exception("Unable to search for option symbols")

        options_df = pd.DataFrame(search_resp['values'])
        
        call_token = options_df[options_df['tsym'] == call_symbol]['token'].values[0]
        put_token = options_df[options_df['tsym'] == put_symbol]['token'].values[0]

        return {
            'call': {'token': call_token, 'symbol': call_symbol},
            'put': {'token': put_token, 'symbol': put_symbol},
            'strike': atm_strike
        }

    except Exception as e:
        print(f"Error getting option tokens: {str(e)}")
        return None

def get_option_data(api, option_info):
    """Get and display option chain data"""
    try:
        # Get Call option data
        call_quote = api.get_quotes(exchange='NFO', token=option_info['call']['token'])
        # Get Put option data
        put_quote = api.get_quotes(exchange='NFO', token=option_info['put']['token'])

        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        
        print(f"\n{timestamp} - Strike Price: {option_info['strike']}")
        print("\nCALL Option:")
        print(f"Symbol: {option_info['call']['symbol']}")
        print(f"LTP: {call_quote.get('lp', 'N/A')}")
        print(f"Change: {call_quote.get('c', 'N/A')}")
        print(f"OI: {call_quote.get('oi', 'N/A')}")
        print(f"Volume: {call_quote.get('v', 'N/A')}")
        
        print("\nPUT Option:")
        print(f"Symbol: {option_info['put']['symbol']}")
        print(f"LTP: {put_quote.get('lp', 'N/A')}")
        print(f"Change: {put_quote.get('c', 'N/A')}")
        print(f"OI: {put_quote.get('oi', 'N/A')}")
        print(f"Volume: {put_quote.get('v', 'N/A')}")

        return {
            'call': call_quote,
            'put': put_quote,
            'timestamp': timestamp
        }

    except Exception as e:
        print(f"Error fetching option data: {str(e)}")
        return None

def main():
    try:
        # Initialize API
        api = initialize_api()
        if not api:
            raise Exception("Failed to initialize API")

        print("Starting Nifty Options Monitor...")
        
        while True:
            if not is_market_open():
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"\r{current_time} - Market is closed. Waiting for market hours (09:15 - 15:30)...", end="")
                time.sleep(60)
                continue

            # Get option tokens
            option_info = get_option_tokens(api)
            if not option_info:
                print("Error getting option tokens. Retrying in 5 seconds...")
                time.sleep(5)
                continue

            # Get and display option data
            option_data = get_option_data(api, option_info)
            if not option_data:
                print("Error fetching option data. Retrying in 5 seconds...")
                time.sleep(5)
                continue

            # Wait for 1 minute before next update
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nStopping the options monitor...")
    except Exception as e:
        print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()