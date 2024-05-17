from datetime import datetime, timedelta
import pandas as pd

from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv
from param import tokens_list

'''exp setting'''
tokens = tokens_list
end_time_str = '2024-05-14'
time_winodw = 1
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'

def save_volume():
    res = []
    for token in tokens:
        print(f"Now {token}: {tokens.index(token) + 1} of {len(tokens)}")

        df_spot, exchange_spot = get_spot_ohlcv(token, start_time_str, end_time_str)
        df_perp, exchange_perp, symbol_perp = get_perp_ohlcv(token, start_time_str, end_time_str)
        volume_spot = (df_spot["volume"] * df_spot["close"]).sum()
        volume_perp = (df_perp["volume"] * df_perp["close"]).sum()

        if df_spot is None or df_perp is None:
            continue
        res.append([token, volume_spot, volume_perp, exchange_spot, exchange_perp])
        df = pd.DataFrame(res, columns=["token", "spot_volume_24h", "perp_volume_24h", "exchange_spot", "exchange_perp"])
        df.to_excel(f"Volume_{start_time_str}_{end_time_str}.xlsx", index=False)

if __name__ == '__main__':
    save_volume()