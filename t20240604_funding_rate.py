import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from data_common.common import cxxtSymbol_to_exchangeSymbol
from data_common.crawl_binance_requests import get_binance_funding_rate
from data_common.crawl_bybit_requests import get_bybit_funding_rate
from data_common.crawl_okx_requests import get_okx_funding_rate
from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv, perp_symbol_proposal
from param import tokens_list, bybit_token_list, okx_token_list, binance_token_list, cap_top150_tokens
from tools.date_util import datetime_to_timestamp_tz0
from tools.dir_util import project_dir

'''exp setting'''
# tokens = ["BTC", "ETH", "SOL", "AVAX", "PEPE", "LINK", "ARB", "NEAR", "BONK", "DOGE", "ORDI", "ENS", "FIL", "LDO"]
exchange = "binance"
if exchange == "bybit":
    tokens = bybit_token_list + ["WIF", "TAO"] + ["STRK"] + ['OMNI', 'MEW', 'SAFE', 'BB', 'UMA', 'MOVR', 'VANRY', 'WIF', 'LEVER']
elif exchange == "okx":
    tokens = okx_token_list
elif exchange == "binance":
    tokens = binance_token_list + ["HBAR"]
else:
    raise ValueError("exchange error.")
tokens = set(tokens).intersection(set(cap_top150_tokens))
tokens = sorted(list(tokens))
print(f"{len(tokens)} tokens funding rate on {exchange} needs to craw.")
end_time_str = '2024-06-17'
time_window = 168 + 365
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_window)
start_time_str = start_time_str.strftime("%Y-%m-%d")
print(f"From {start_time_str} to {end_time_str}...")

exchanges = [exchange]
item_num = 3 * time_window + 1

def save_funding_rate():
    start_time_ts = datetime_to_timestamp_tz0(datetime.strptime(start_time_str, "%Y-%m-%d"))
    end_time_ts = datetime_to_timestamp_tz0(datetime.strptime(end_time_str, "%Y-%m-%d"))

    res = {}
    idx = None
    for token in tokens:
        print(f"Now {token}: {tokens.index(token) + 1} of {len(tokens)}")
        symbol_proposal = perp_symbol_proposal(token)
        fr_list = None

        for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
            try:
                isymbol = cxxtSymbol_to_exchangeSymbol(exchange, symbol)
                if exchange == "bybit":
                    df = get_bybit_funding_rate(isymbol, "linear", start_time=start_time_ts, end_time=end_time_ts)
                elif exchange == "okx":
                    isymbol = isymbol + "-SWAP"
                    df = get_okx_funding_rate(isymbol, None, start_time=start_time_ts, end_time=end_time_ts+1)
                elif exchange == "binance":
                    df = get_binance_funding_rate(isymbol, None, start_time=start_time_ts, end_time=end_time_ts)
                    df = df.iloc[::-1].reset_index(drop=True)
                else:
                    raise ValueError("Exchange Error.")

                df.rename(columns={"fundingRateTimestamp": "open_time"}, inplace=True)
                df["open_time"] = df["open_time"].apply(lambda x: str(x)[:19])
                df["ts"] = df["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df["mod"] = df["ts"].apply(lambda x: np.mod(x, 86400000/3))
                df_ind = df["mod"]!=0
                if sum(df_ind)!=0:
                    print(f"remove {sum(df_ind)} rows of {symbol} in {exchange}: {df.loc[df_ind, 'open_time'].to_list()}")
                    df = df.loc[~df_ind, :]
                df = df.iloc[::-1].reset_index(drop=True)
                fr_list = df["fundingRate"].to_list()
                if idx is None and len(df) == item_num:
                    idx = df["open_time"].to_list()
                if len(df) == 0:
                    continue
                break
            except:
                print(f"No {symbol} in {exchange}...")
                continue
        if fr_list is None:
            continue
        if len(fr_list) != item_num:
            new_fr_list = [None for i in range(item_num)]
            new_fr_list[-len(fr_list):] = fr_list
            fr_list = new_fr_list
        res[token] = fr_list
        df = pd.DataFrame(res)
        if idx is None:
            idx = pd.date_range(start=f'{start_time_str} 00:00:00', end=f'{end_time_str} 00:00:00', freq='8H')
            idx = idx.strftime('%Y-%m-%d %H:%M:%S')

        df.index = idx
        csv_name = os.path.join(project_dir(), "data", "a_funding", exchange,
                                 f"FundingRate-{exchange}-{len(tokens)}tokens_{start_time_str}_{end_time_str}.csv")
        df.to_csv(csv_name)

if __name__ == '__main__':
    save_funding_rate()
