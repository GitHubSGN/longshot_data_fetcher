import pandas as pd

from crawl_common import get_ohlcv_df
from param import tokens_list

# tokens = ["TAO", "1000PEPE", "ARB", "WIF", "LDO", "GALA", "OP", "LINK", "AVAX", "NEAR", "SOL", "FIL", "ORDI", "TON"]

tokens = tokens_list
start_time_str = '2024-04-30'
end_time_str = '2024-05-13'
exchanges = ['bybit', "binance", "OKX"]
timeframe = '1h'
limit = 1000

def get_mean_spread(token: str):
    symbol = f"{token}/USDT"
    df_spot = None
    for exchange in exchanges:
        try:
            df_spot = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue
    symbol = f'{token}/USDT:USDT'
    df_perp = None
    for exchange in exchanges:
        try:
            df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    if df_spot is None or df_perp is None:
        return None, None
    else:
        df = pd.merge( df_spot[["open_time", "close"]], df_perp[["open_time", "close"]], on="open_time", how="inner", suffixes=["Spot", "Perps"] )
        df["spread"] = (df["closePerps"] - df["closeSpot"]) / (df["closeSpot"]) * 10000
        return df["spread"].mean(), df["spread"].values[-1]

def cal_save_long_short_spread():
    res = []
    for token in tokens:
        ms, ls = get_mean_spread(token)
        if ms is None:
            continue
        res.append([token, ms, ls])
        df = pd.DataFrame(res, columns=["token", "Bs","MeanBs"])
        df.to_excel("20240514.xlsx", index=False)

if __name__ == "__main__":
    cal_save_long_short_spread()
