import re
import pandas as pd
from datetime import datetime, timedelta

from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv
from func_common.basis_spread import cal_basis_spread
from param import tokens_list

'''exp setting'''
# tokens = ["TAO", "1000PEPE", "ARB", "WIF", "LDO", "GALA", "OP", "LINK", "AVAX", "NEAR", "SOL", "FIL", "ORDI", "TON"]
# tokens = ["MASK","MANA","FTM","BCH","SATS"]
# tokens = ["SATS"]
tokens = tokens_list        # tokens_1000_list
end_time_str = '2024-05-14'
time_winodw = 30
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def get_mean_spread(token: str):
    print(f"Now {token}: {tokens.index(token)+1} of {len(tokens)}")

    '''spot price history'''
    df_spot, exchange_spot = get_spot_ohlcv(token, start_time_str, end_time_str)

    '''perps price history'''
    df_perp, exchange_perp, symbol_perp = get_perp_ohlcv(token, start_time_str, end_time_str)

    '''average spread &. last spread'''
    if df_spot is None or df_perp is None:
        return None, None, None, None
    else:
        ms, ls, _ = cal_basis_spread(df_spot, df_perp, symbol_perp)
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
