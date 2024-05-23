import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_common.crawl_common import get_ohlcv_df
from param import tokens_list

'''exp setting'''
tokens = tokens_list        # tokens_1000_list
end_time_str = '2024-05-21'
time_winodw = 14
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def get_vol(token: str):
    print(f"Now {token}: {tokens.index(token)+1} of {len(tokens)}")

    '''spot price history'''
    symbol = f"{token}/USDT"
    df_spot = None
    exchange_spot = None
    for exchange in exchanges:
        try:
            df_spot = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
            exchange_spot = exchange
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    if df_spot is None:
        return None, None
    else:
        vol = np.log(df_spot["close"]).diff().std() * np.sqrt( pd.to_timedelta("365d") / pd.to_timedelta(timeframe)  )
        return vol, exchange_spot

def cal_save_volatility():
    res = []
    for token in tokens:
        vol, e = get_vol(token)
        if vol is None:
            continue
        res.append([token, vol, e])
        df = pd.DataFrame(res, columns=["token", "vol(p.a.)", "exchange"])
        df.to_excel(f"VOL_{start_time_str}_{end_time_str}.xlsx", index=False)


if __name__ == "__main__":
    cal_save_volatility()