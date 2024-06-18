from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from data_common.crawl_bybit_requests import get_bybit_funding_rate
from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv, perp_symbol_proposal
from param import tokens_list, bybit_token_list, okx_token_list, binance_token_list, cap_top150_tokens
from tools.date_util import datetime_to_timestamp_tz0

'''exp setting'''
# tokens = ["BTC", "ETH", "SOL", "AVAX", "PEPE", "LINK", "ARB", "NEAR", "BONK", "DOGE", "ORDI", "ENS", "FIL", "LDO"]
exchange = "bybit"
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
tokens = tokens[46:]
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
                isymbol = symbol.split(":")[0].replace(f"/", "")
                df = get_bybit_funding_rate(isymbol, "linear", start_time=start_time_ts, end_time=end_time_ts)
                df.rename(columns={"fundingRateTimestamp": "open_time"}, inplace=True)
                df["open_time"] = df["open_time"].apply(lambda x: str(x))
                df["ts"] = df["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df["mod"] = df["ts"].apply(lambda x: np.mod(x, 86400000/3))
                df_ind = df["mod"]!=0
                if sum(df_ind)!=0:
                    print(f"remove {sum(df_ind)} rows of {symbol} in {exchange}: {df.loc[df_ind, 'open_time'].to_list()}")
                    df = df.loc[~df_ind, :]
                df = df.iloc[::-1]
                fr_list = df["fundingRate"].to_list()
                if idx is None and len(df) == item_num:
                    idx = df["open_time"].to_list()
                if len(df) == 0:
                    continue
                break
            except:
                print(f"No {symbol} in {exchange}...")
                continue
        if len(fr_list) != item_num:
            new_fr_list = [None for i in range(item_num)]
            new_fr_list[-len(fr_list):] = fr_list
            fr_list = new_fr_list
        res[token] = fr_list
        df = pd.DataFrame(res)
        if idx is not None:
            df.index = idx
        df.to_excel(f"FundingRate-{len(tokens)}tokens_{start_time_str}_{end_time_str}.xlsx")

if __name__ == '__main__':
    save_funding_rate()
