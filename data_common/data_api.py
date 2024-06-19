import os
import pandas as pd
from datetime import datetime

from data_common.crawl_bybit_requests import get_bybit_open_interest, get_bybit_funding_rate
from data_common.crawl_common import get_ohlcv_df
from param import tokens_list, tokens_multiplier_dict, data_dir, bybit_token_list, cap_top150_tokens
from tools.date_util import datetime_to_timestamp_tz0
from tools.dir_util import create_directory

'''exp setting'''
end_time_str = '2024-06-19'
start_time_str = '2023-01-01'
# start_time_str = '2024-05-13'
exchanges_prop = ['bybit']
timeframe = '1h'
limit = 1000

def perp_symbol_proposal(token: str):
    symbol_proposal = [token]
    for multiplier, tokens_multiplier_list in tokens_multiplier_dict.items():
        if token in tokens_multiplier_list:
            symbol_proposal = [f"{multiplier}{token}", f"{token}{multiplier}",token] if token in tokens_multiplier_list else [token]
            break
    symbol_proposal = [f'{token}/USDT:USDT' for token in symbol_proposal]
    return symbol_proposal


def get_spot_ohlcv(token: str, start_str: str = start_time_str, end_str: str = end_time_str, exchange: str = None):
    exchanges = [exchange] if exchange is not None else exchanges_prop

    '''spot price history'''
    symbol = f"{token}/USDT"
    df_spot = None
    exchange_spot = None
    for exchange in exchanges:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_str}-{end_str}", f"{token}_spot_{exchange}_{start_time_str}_{start_time_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df_spot = pd.read_excel(xlsx_fn, index_col=False)
            else:
                df_spot = get_ohlcv_df(symbol, start_str, end_str, exchange, timeframe, limit)
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
        df_spot.reset_index(drop=True, inplace=True)
    return df_spot, exchange_spot

def get_perp_ohlcv(token: str, start_str: str = start_time_str, end_str: str = end_time_str, exchange: str = None):
    exchanges = [exchange] if exchange is not None else exchanges_prop

    '''perps price history'''
    df_perp = None
    exchange_perp, symbol_perp = None, None
    symbol_proposal = perp_symbol_proposal(token)
    for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_str}-{end_str}", f"{token}_perp_{exchange}_{start_str}_{start_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df_perp = pd.read_excel(xlsx_fn, index_col=False)
            else:
                df_perp = get_ohlcv_df(symbol, start_str, end_str, exchange, timeframe, limit)
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
        df_perp.reset_index(drop=True, inplace=True)
    return df_perp, exchange_perp, symbol_perp

def get_open_interest(token: str, start_str: str = start_time_str, end_str: str = end_time_str, exchange: str = 'bybit'):
    if exchange != 'bybit':
        raise ValueError("Sorry. Only Bybit is allowed to get openinterest.")

    start_time_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_time_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))

    df = None
    exchanges = [exchange] if exchange is not None else exchanges_prop
    symbol_proposal = perp_symbol_proposal(token)
    for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_str}-{end_str}", f"{token}_perp_{exchange}_oi_{start_time_str}_{start_time_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df = pd.read_excel(xlsx_fn, index_col=False)
            else:
                isymbol = symbol.split(":")[0].replace(f"/", "")
                df = get_bybit_open_interest(isymbol, "linear", start_time=start_time_ts, end_time=end_time_ts)
                df.rename(columns={"timestamp":"open_time"}, inplace=True)
                df["open_time"] = df["open_time"].apply(lambda x: str(x))
                df["ts"] = df["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df_perp, _, _ = get_perp_ohlcv(token, start_str, end_str, exchange)
                df = pd.merge( df_perp[["ts", "open"]], df, on="ts", how="inner" )
                df["oi"] = df["oi"] * df["open"]
                del df["open"]
                del df["category"]
                df.to_excel(xlsx_fn, index=False)
            if len(df)==0:
                df = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    start_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))
    if df is not None:
        df = df.loc[ (df["ts"]>=start_ts) & (df["ts"]<end_ts), : ]
        df.reset_index(drop=True, inplace=True)
    return df

def get_funding_rate(token: str, start_str: str = start_time_str, end_str: str = end_time_str, exchange: str = 'bybit'):
    if exchange != 'bybit':
        raise ValueError("Sorry. Only Bybit is allowed to get funding rate.")

    start_time_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_time_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))

    df = None
    exchanges = [exchange] if exchange is not None else exchanges_prop
    symbol_proposal = perp_symbol_proposal(token)
    for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
        try:
            xlsx_fn = os.path.join(data_dir, f"{start_str}-{end_str}", f"{token}_perp_{exchange}_fr_{start_time_str}_{start_time_str}.xlsx")
            create_directory(xlsx_fn)

            if os.path.exists(xlsx_fn):
                df = pd.read_excel(xlsx_fn, index_col=False)
            else:
                isymbol = symbol.split(":")[0].replace(f"/", "")
                df = get_bybit_funding_rate(isymbol, "linear", start_time=start_time_ts, end_time=end_time_ts)
                df.rename(columns={"fundingRateTimestamp":"open_time"}, inplace=True)
                df["open_time"] = df["open_time"].apply(lambda x: str(x))
                df["ts"] = df["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df = df.iloc[::-1]
                df.to_excel(xlsx_fn, index=False)
            if len(df)==0:
                df = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    start_ts = datetime_to_timestamp_tz0(datetime.strptime(start_str, "%Y-%m-%d"))
    end_ts = datetime_to_timestamp_tz0(datetime.strptime(end_str, "%Y-%m-%d"))
    if df is not None:
        df = df.loc[ (df["ts"]>=start_ts) & (df["ts"]<end_ts), : ]
        df.reset_index(drop=True, inplace=True)
    return df

def get_ticksize():
    xlsx_fn = os.path.join(data_dir, f"2024-04-30_2024-05-14", f"132token", f"TickSize_2024-05-13_2024-05-14.xlsx")
    df = pd.read_excel(xlsx_fn, index_col=False)

    return df

def get_raw_data(token: str, start_str: str = start_time_str, end_str: str = end_time_str):
    """
    spot ohlcv, perp ohlcv, open interest, funding rate
    :return: pd.DataFrame
    """
    df_spot, _ = get_spot_ohlcv(token, start_str = start_str, end_str = end_str, exchange="bybit")
    df_perp, _, symbol_perp = get_perp_ohlcv(token, start_str = start_str, end_str = end_str, exchange="bybit")
    df_fr = get_funding_rate(token, start_str = start_str, end_str = end_str)
    df_oi = get_open_interest(token, start_str = start_str, end_str = end_str)
    df_spot.rename(columns = {"open":"so", "high":"sh", "low":"sl", "close":"sc", "volume":"sv"}, inplace = True)
    print("Done")

    return True


def save_all_token_data():
    tokens = bybit_token_list + ["WIF", "TAO"] + ["STRK"] + ['OMNI', 'MEW', 'SAFE', 'BB', 'UMA', 'MOVR', 'VANRY', 'WIF','LEVER']
    tokens = set(tokens).intersection(set(cap_top150_tokens))
    token_list = sorted(list(tokens))
    for token in tokens_list:
    # for token in ["BTC"]:
        print(f"Now {token} Spot: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        df, exchanges_spot = get_spot_ohlcv(token, start_time_str, end_time_str)
        print(f"Now {token} Perps: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        df, exchanges_perp, symbol_perp = get_perp_ohlcv(token, start_time_str, end_time_str)
        # print(f"Now {token} OI: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        # df = get_funding_rate(token, start_time_str, end_time_str)

        # print(f"Now {token} OI: {tokens_list.index(token) + 1} of {len(tokens_list)}")
        # df = get_funding_rate(token, start_time_str, end_time_str)


if __name__ == "__main__":
    save_all_token_data()
    df = get_ticksize()
    # df = get_raw_data("BTC")
    # print("Success.")
