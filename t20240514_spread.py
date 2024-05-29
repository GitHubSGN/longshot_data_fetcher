import re
import pandas as pd
from datetime import datetime, timedelta

from data_common.crawl_common import get_ohlcv_df
from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv
from func_common.basis_spread import cal_basis_spread
from param import tokens_list, tokens_multiplier_dict

'''exp setting'''
# tokens = ["TAO", "1000PEPE", "ARB", "WIF", "LDO", "GALA", "OP", "LINK", "AVAX", "NEAR", "SOL", "FIL", "ORDI", "TON"]
# tokens = ["MASK","MANA","FTM","BCH","SATS"]
# tokens = ["SATS"]
tokens = tokens_list        # tokens_1000_list
end_time_str = '2024-05-21'
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
            if len(df_spot)==0:
                df_spot = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

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
            df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
            exchange_perp = exchange
            symbol_perp = symbol
            if len(df_perp)==0:
                df_perp = None
                continue
            break
        except:
            print(f"No {symbol} in {exchange}...")
            continue

    '''average spread &. last spread'''
    if df_spot is None or df_perp is None:
        return None, None, None, None
    else:
        ms, ls, _, _ = cal_basis_spread(df_spot, df_perp, symbol_perp)
        return ms, ls, exchange_spot, exchange_perp

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
