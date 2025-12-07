from market_data import initialize_api
from config import NIFTY_STRIKE_STEP, BANKNIFTY_STRIKE_STEP
import datetime, pandas as pd, os, time

FETCH_DATE = '03-12-2025'
NIFTY_EXPIRY = '09DEC25'
BANKNIFTY_EXPIRY = '30DEC25'
NIFTY_ATM_OVERRIDE = None
BANKNIFTY_ATM_OVERRIDE = None

UNDERLYING_CONFIG = [
    {'name': 'NIFTY', 'index_token': '26000', 'strike_step': NIFTY_STRIKE_STEP, 'expiry': NIFTY_EXPIRY, 'atm_override': NIFTY_ATM_OVERRIDE},
    {'name': 'Nifty Bank', 'index_token': '26009', 'strike_step': BANKNIFTY_STRIKE_STEP, 'expiry': BANKNIFTY_EXPIRY, 'atm_override': BANKNIFTY_ATM_OVERRIDE}
]

DATA_DIR = os.path.join(os.getcwd(), 'data', 'market_data')
os.makedirs(DATA_DIR, exist_ok=True)

def search_specific_contract(api, symbol):
    resp = api.searchscrip(exchange='NFO', searchtext=symbol)
    if not resp or resp.get('stat') != 'Ok': return None, None
    for v in resp.get('values', []):
        tsym = v.get('tsym', '')  # Fix: Define tsym variable
        if tsym == symbol:
            return v.get('token'), tsym  # Now tsym is defined
    return None, None

def get_time(time_string):
    data = time.strptime(time_string, '%d-%m-%Y %H:%M:%S')
    return time.mktime(data)

def main():
    api = initialize_api()
    if not api: raise SystemExit("API initialization failed")

    start_datetime = FETCH_DATE + ' 09:15:00'
    end_datetime = FETCH_DATE + ' 15:30:00'
    start_timestamp = get_time(start_datetime)
    end_timestamp = get_time(end_datetime)

    print(f"\n{'='*70}\nCONFIGURATION\n{'='*70}")
    print(f"Fetch Date: {FETCH_DATE}\nTime Range: {start_datetime} to {end_datetime}")
    print(f"Timestamps: {int(start_timestamp)} to {int(end_timestamp)}")
    print(f"NIFTY Expiry: {NIFTY_EXPIRY}\nBANKNIFTY Expiry: {BANKNIFTY_EXPIRY}\n{'='*70}")
    
    print(f"\n{'='*70}\nTESTING API DATA AVAILABILITY\n{'='*70}")
    
    print(f"\nTesting NIFTY Index live quotes...")
    test_nifty = api.get_quotes(exchange='NSE', token='26000')
    if test_nifty and test_nifty.get('stat') == 'Ok':
        print(f"  ✓ NIFTY LTP: {test_nifty.get('lp', 'N/A')}\n  Feed Time: {test_nifty.get('ft', 'N/A')}")
    else: print(f"  ⚠ Failed to get NIFTY quotes")
    
    print(f"\nTesting BANKNIFTY Index live quotes...")
    test_banknifty = api.get_quotes(exchange='NSE', token='26009')
    if test_banknifty and test_banknifty.get('stat') == 'Ok':
        print(f"  ✓ BANKNIFTY LTP: {test_banknifty.get('lp', 'N/A')}\n  Feed Time: {test_banknifty.get('ft', 'N/A')}")
    else: print(f"  ⚠ Failed to get BANKNIFTY quotes")
    
    print(f"\nTesting NIFTY Index historical data for {FETCH_DATE}...")
    try:
        test_history = api.get_time_price_series(exchange='NSE', token='26000', starttime=start_timestamp, endtime=end_timestamp)
        if test_history and isinstance(test_history, list) and len(test_history) > 0:
            print(f"  ✓ NIFTY historical data available: {len(test_history)} candles\n  First candle: {test_history[0]}\n  Last candle: {test_history[-1]}")
        elif isinstance(test_history, dict): print(f"  ⚠ API returned error: {test_history}")
        else: print(f"  ⚠ No historical data for NIFTY on {FETCH_DATE}\n  Response: {test_history}\n\n  CRITICAL: {FETCH_DATE} appears to be a market holiday or data not available!\n  Please change FETCH_DATE to a valid trading day.")
    except Exception as e: print(f"  ✗ Error fetching historical data: {e}")
    
    print(f"\n{'='*70}\nPROCEEDING WITH OPTION DATA FETCH\n{'='*70}")

    for config in UNDERLYING_CONFIG:
        underlying = config['name']; index_token = config['index_token']; strike_step = config['strike_step']; expiry_str = config['expiry']
        print(f"\n{'='*70}\nProcessing {underlying}\n{'='*70}")
        print(f"Fetching live LTP for {underlying}...")
        idx_q = api.get_quotes(exchange='NSE', token=index_token)
        if not idx_q or idx_q.get('stat') != 'Ok': print(f"Failed to get quotes for {underlying}"); continue
        ltp = float(idx_q.get('lp', 0)); print(f"LTP from API: {ltp:.2f}")
        if ltp < 10000: print(f"⚠ LTP seems incorrect, trying alternative method..."); ltp = float(input(f"Enter current LTP for {underlying} (or press Enter to use {ltp}): ") or ltp)
        atm = int(round(ltp / strike_step) * strike_step); print(f"Final LTP: {ltp:.2f} | ATM Strike: {atm}")
        base = 'BANKNIFTY' if 'Bank' in underlying or 'BANK' in underlying.upper() else 'NIFTY'
        try: expiry_date = datetime.datetime.strptime(expiry_str, '%d%b%y').date(); expiry_label = expiry_date.strftime('%d-%b-%Y')
        except: expiry_label = expiry_str; expiry_date = None
        print(f"Expiry: {expiry_label} ({expiry_str})")
        strikes = [atm - strike_step, atm, atm + strike_step]; print(f"Selected strikes (ATM ±1): {strikes}\n")
        combined_rows = []; success_count = 0
        for strike in strikes:
            for typ in ['C', 'P']:
                symbol = f"{base}{expiry_str}{typ}{strike}"; print(f"  Searching for: {symbol}")
                token, found_symbol = search_specific_contract(api, symbol)
                if not token: print(f"    ⚠ Contract not found (may not be listed yet)"); continue
                print(f"    ✓ Found | Token: {token}")
                try: ret = api.get_time_price_series(exchange='NFO', token=token, starttime=start_timestamp, endtime=end_timestamp)
                except Exception as e: print(f"    ⚠ API Error: {e}"); ret = None
                if not ret: print(f"    ⚠ No data returned"); continue
                if isinstance(ret, dict):
                    if ret.get('stat') == 'Not_Ok': print(f"    ⚠ API Error: {ret.get('emsg', 'Unknown')}"); continue
                try: df = pd.DataFrame(ret)
                except Exception as e: print(f"    ⚠ DataFrame error: {e}"); continue
                if df.empty: print(f"    ⚠ Empty dataframe (no trading activity)"); continue
                print(f"    ✓ Data: {len(df)} candles")
                if success_count == 0: print(f"    Columns: {list(df.columns)}")
                column_map = {'time': 'time', 'ssboe': 'time', 'into': 'open', 'inth': 'high', 'intl': 'low', 'intc': 'close', 'v': 'volume', 'intv': 'volume', 'intoi': 'open_interest', 'oi': 'open_interest'}
                df.rename(columns=column_map, inplace=True)
                df['Underlying'] = underlying; df['Expiry'] = expiry_date.strftime('%Y-%m-%d') if expiry_date else expiry_str; df['Strike'] = strike; df['Type'] = typ
                filename = f"{symbol}.csv"; outfile = os.path.join(DATA_DIR, filename); df.to_csv(outfile, index=False); print(f"    ✓ Saved: {filename}")
                combined_rows.append(df); success_count += 1; time.sleep(0.5)
        if combined_rows:
            df_combined = pd.concat(combined_rows, ignore_index=True); date_str = FETCH_DATE.replace('-', '')
            expiry_file_str = expiry_date.strftime('%Y%m%d') if expiry_date else expiry_str
            outname = f"{base}_{expiry_file_str}_combined_{date_str}.csv"; outfile = os.path.join(DATA_DIR, outname); df_combined.to_csv(outfile, index=False)
            print(f"\n{'='*60}\n✓✓ COMBINED FILE SAVED\n   File: {outname}\n   Total rows: {len(df_combined)}\n   Successful contracts: {success_count}/6\n{'='*60}")
        else: print(f"\n{'='*60}\n⚠⚠ NO DATA COLLECTED FOR {underlying}\n\nPossible reasons:\n  1. November 28, 2025 was a market holiday\n  2. These strikes had ZERO trading activity\n  3. Contracts not yet listed for this expiry\n  4. API data not available for that date\n\nTroubleshooting:\n  • Check if market was open on {FETCH_DATE}\n  • Try a different date (e.g., '26-11-2024', '25-11-2024')\n  • Verify expiry {expiry_str} has listed contracts\n{'='*60}")

    print(f"\n{'='*70}\nFetch completed!\nOutput directory: {DATA_DIR}\n{'='*70}\n")

if __name__ == "__main__": main()