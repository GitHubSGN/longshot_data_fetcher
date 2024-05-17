import os
import pandas as pd
from datetime import datetime

from crawl_common import get_ohlcv_df
from param import tokens_list, tokens_multiplier_dict, data_dir
from tools.date_util import datetime_to_timestamp_tz0
from tools.dir_util import create_directory

'''exp setting'''
end_time_str = '2024-05-16'
start_time_str = '2024-01-01'
# start_time_str = '2024-05-13'
exchanges_prop = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def get_spot_ohlcv(token: str, start_str: str, end_str: str, exchange: str = None):
    exchanges = [exchange] if exchange is not None else exchanges_prop

    '''spot price history'''
    symbol = f"{token}/USDT"
    df_spot = None
    exchange_spot = None
    for exchange in exchanges:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_time_str}-{end_time_str}", f"{token}_spot_{exchange}_{start_time_str}_{start_time_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df_spot = pd.read_excel(xlsx_fn, index_col=False)
            else:
                df_spot = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
                df_spot["ts"] = df_spot["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df_spot.to_excel(xlsx_fn, index=False)
            exchange_spot = exchange
            if len(df_spot)==0:
                df_spot = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    start_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))
    if df_spot is not None:
        df_spot = df_spot.loc[ (df_spot["ts"]>=start_ts) & (df_spot["ts"]<end_ts), : ]
    return df_spot, exchange_spot


def get_perp_ohlcv(token: str, start_str: str, end_str: str, exchange: str = None):
    exchanges = [exchange] if exchange is not None else exchanges_prop

    '''perps price history'''
    df_perp = None
    exchange_perp, symbol_perp = None, None
    symbol_proposal = [token]
    for multiplier, tokens_multiplier_list in tokens_multiplier_dict.items():
        if token in tokens_multiplier_list:
            symbol_proposal = [f"{multiplier}{token}", f"{token}{multiplier}", token] if token in tokens_multiplier_list else [token]
            break
    symbol_proposal = [f'{token}/USDT:USDT' for token in symbol_proposal]
    for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_time_str}-{end_time_str}", f"{token}_perp_{exchange}_{start_time_str}_{start_time_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df_perp = pd.read_excel(xlsx_fn, index_col=False)
            else:
                df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
                df_perp["ts"] = df_perp["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df_perp.to_excel(xlsx_fn, index=False)
            exchange_perp = exchange
            symbol_perp = symbol
            if len(df_perp)==0:
                df_perp = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    start_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))
    if df_perp is not None:
        df_perp = df_perp.loc[ (df_perp["ts"]>=start_ts) & (df_perp["ts"]<end_ts), : ]
    return df_perp, exchange_perp, symbol_perp

def save_all_token_spot_perps():
    for token in tokens_list:
        print(f"Now {token} Spot: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        df, exchanges_spot = get_spot_ohlcv(token, start_time_str, end_time_str)
        print(f"Now {token} Perps: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        df, exchanges_perp, symbol_perp = get_perp_ohlcv(token, start_time_str, end_time_str)

if __name__ == "__main__":
    save_all_token_spot_perps()
