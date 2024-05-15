import pandas as pd
from datetime import datetime, timedelta

from crawl_common import get_ohlcv_df
from param import tokens_list, tokens_1000_list


'''exp setting'''
# tokens = ["TAO", "1000PEPE", "ARB", "WIF", "LDO", "GALA", "OP", "LINK", "AVAX", "NEAR", "SOL", "FIL", "ORDI", "TON"]
tokens = tokens_list        # tokens_1000_list
end_time_str = '2024-05-14'
time_winodw = 14
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def get_mean_spread(token: str):
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

    '''perps price history'''
    df_perp = None
    exchange_perp, symbol_perp = None, None
    symbol_proposal = [f"1000{token}", f"{token}1000", token] if token in tokens_1000_list else [token]
    symbol_proposal = [f'{token}/USDT:USDT' for token in symbol_proposal]
    for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
        try:
            df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
            exchange_perp = exchange
            symbol_perp = symbol
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    '''average spread &. last spread'''
    if df_spot is None or df_perp is None:
        return None, None
    else:
        if symbol_perp.startswith("1000") or symbol_perp.split(f"/")[0].endswith("1000"):
            df_spot[['open', 'high', 'low', 'close']] = df_spot[['open', 'high', 'low', 'close']] * 1000
            df_spot["volume"] = df_spot["volume"] / 1000
        df = pd.merge( df_spot[["open_time", "close"]], df_perp[["open_time", "close"]], on="open_time", how="inner", suffixes=["Spot", "Perps"] )
        df["spread"] = (df["closeSpot"] - df["closePerps"]) / (df["closePerps"]) * 10000
        return df["spread"].mean(), df["spread"].values[-1], exchange_spot, exchange_perp

def cal_save_long_short_spread():
    res = []
    for token in tokens:
        ms, ls, es, ep = get_mean_spread(token)
        if ms is None:
            continue
        res.append([token, ls, ms, es, ep])
        df = pd.DataFrame(res, columns=["token", "Bs", "MeanBs", "exchange_spot", "exchange_perp"])
        df.to_excel(f"Basis_{start_time_str}_{end_time_str}.xlsx", index=False)

if __name__ == "__main__":
    cal_save_long_short_spread()
