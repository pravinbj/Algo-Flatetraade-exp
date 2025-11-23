"""
Fetch 1-min intraday OHLC + summary for ATM ±2 option strikes of NIFTY and BANKNIFTY
Auto-selects strikes around ATM based on live index LTP
Saves CSV in data/market_data/<UNDERLYING>_<EXPIRY>_options_<YYYYMMDD>.csv
"""

from market_data import initialize_api
from config import NIFTY_STRIKE_STEP, BANKNIFTY_STRIKE_STEP, LOT_SIZE_NIFTY, LOT_SIZE_BANKNIFTY
import datetime
import pandas as pd
import os
import time
import bisect

UNDERLYING_CONFIG = [
    {'name': 'NIFTY', 'index_token': '26000', 'strike_step': NIFTY_STRIKE_STEP, 'lot_size': LOT_SIZE_NIFTY},
    {'name': 'Nifty Bank', 'index_token': '26009', 'strike_step': BANKNIFTY_STRIKE_STEP, 'lot_size': LOT_SIZE_BANKNIFTY}
]

DATA_DIR = os.path.join(os.getcwd(), 'data', 'market_data')
os.makedirs(DATA_DIR, exist_ok=True)


def find_option_token(values, expiry_str, strike, typ, underlying):
    """Find the token for the given strike and type (C/P)."""
    if 'Bank' in underlying:
        underlying_normalized = 'BANKNIFTY'
    else:
        underlying_normalized = underlying.upper()
    target = f"{underlying_normalized}{expiry_str}{typ}{strike}"

    for v in values:
        if v.get('tsym', '') == target:
            return v.get('token'), v.get('tsym')
    return None, None


def fetch_ohlc(api, token):
    """Fetch 1-min OHLC for today, fallback to previous trading day."""
    today = datetime.date.today()
    date_str = today.strftime('%Y-%m-%d')

    try:
        ret = api.get_time_price_series(exchange='NFO', token=token, starttime=date_str, endtime=date_str, interval='1')
    except Exception:
        ret = None

    if not ret:
        # fallback to previous trading day
        d = today - datetime.timedelta(days=1)
        while d.weekday() >= 5:
            d -= datetime.timedelta(days=1)
        date_str = d.strftime('%Y-%m-%d')
        try:
            ret = api.get_time_price_series(exchange='NFO', token=token, starttime=date_str, endtime=date_str, interval='1')
        except Exception:
            ret = None

    return ret


def compute_summary(df):
    """Compute summary fields for CSV (Open, High, Low, Close, LTP, % Change, Volume)."""
    if df.empty:
        return None
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime')

    open_ = df['open'].iloc[0]
    high = df['high'].max()
    low = df['low'].min()
    close = df['close'].iloc[-1]
    ltp = close
    vol = df['volume'].sum()
    pct_change = ((close - open_) / open_) * 100 if open_ else 0.0

    return {
        'Open': open_,
        'High': high,
        'Low': low,
        'Close': close,
        'LTP': ltp,
        '% Change': round(pct_change, 2),
        'Volume': vol
    }


def main():
    api = initialize_api()
    if not api:
        raise SystemExit("API initialization failed")

    for config in UNDERLYING_CONFIG:
        underlying = config['name']
        index_token = config['index_token']
        strike_step = config['strike_step']
        lot_size = config['lot_size']

        print(f"\n{'='*70}")
        print(f"Processing {underlying}")
        print(f"{'='*70}")

        idx_q = api.get_quotes(exchange='NSE', token=index_token)
        if not idx_q:
            print(f"Failed to get LTP for {underlying}")
            continue

        ltp = float(idx_q.get('lp', 0))
        atm = int(round(ltp / strike_step) * strike_step)
        print(f"LTP: {ltp:.2f} | ATM Strike: {atm}\n")

        resp = api.searchscrip(exchange='NFO', searchtext=underlying)
        values = resp.get('values', []) if resp else []

        expiries = []
        for v in values:
            exd = v.get('exd')
            if exd:
                for fmt in ("%d-%b-%Y", "%d-%b-%y"):
                    try:
                        expiries.append(datetime.datetime.strptime(exd, fmt).date())
                        break
                    except Exception:
                        continue
        expiries = sorted(set(expiries))
        expiry = min([d for d in expiries if d >= datetime.date.today()], default=max(expiries))
        expiry_str = expiry.strftime('%d%b%y').upper()
        expiry_label = expiry.strftime('%d-%b-%Y').upper()
        print(f"Expiry: {expiry_label}\n")

        # collect available strikes
        if 'Bank' in underlying:
            base = 'BANKNIFTY'
        else:
            base = underlying.upper()

        available = []
        for v in values:
            if v.get('instname') != 'OPTIDX':
                continue
            if v.get('exd', '').upper() != expiry_label:
                continue
            ts = v.get('tsym', '')
            if ts.startswith(base + expiry_str):
                try:
                    strike = int(ts.split('C')[-1]) if 'C' in ts else int(ts.split('P')[-1])
                    available.append(strike)
                except:
                    continue
        available = sorted(set(available))

        def nearest_strikes(available, atm, n=2):
            """Return ATM ± n strikes."""
            idx = bisect.bisect_left(available, atm)
            strikes = []
            if atm in available:
                strikes.append(atm)
            below = available[max(0, idx - n):idx]
            above = available[idx + 1:idx + n + 1]
            return sorted(set(below + strikes + above))

        strikes = nearest_strikes(available, atm, n=2)
        print(f"Selected strikes: {strikes}\n")

        rows = []
        for strike in strikes:
            for typ in ['C', 'P']:
                token, sym = find_option_token(values, expiry_str, strike, typ, underlying)
                if not token:
                    print(f"No token for {strike}{typ}")
                    continue
                ret = fetch_ohlc(api, token)
                if not ret:
                    print(f"No intraday data for {strike}{typ}")
                    continue
                df = pd.DataFrame(ret)
                if df.empty:
                    continue
                summary = compute_summary(df)
                if summary:
                    row = {
                        'Underlying': underlying,
                        'Expiry': expiry.strftime('%Y-%m-%d'),
                        'Strike': strike,
                        'Type': typ,
                        **summary
                    }
                    rows.append(row)
                time.sleep(1)

        if rows:
            df_out = pd.DataFrame(rows)
            outname = f"{underlying.replace(' ', '')}_{expiry.strftime('%Y%m%d')}_options_{datetime.date.today().strftime('%Y%m%d')}.csv"
            outfile = os.path.join(DATA_DIR, outname)
            df_out.to_csv(outfile, index=False)
            print(f"\n✓ Saved summary: {outfile}\n")

    print(f"\n{'='*70}")
    print("Fetch completed.")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
