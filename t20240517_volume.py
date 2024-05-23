from datetime import datetime, timedelta
import pandas as pd

from data_common.crawl_common import get_ohlcv_df
from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv
from param import tokens_list, tokens_multiplier_dict

'''exp setting'''
tokens = tokens_list
end_time_str = '2024-05-21'
time_window = 1
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_window)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def save_volume():
    res = []
    for token in tokens:
        print(f"Now {token}: {tokens.index(token) + 1} of {len(tokens)}")

        # df_spot, exchange_spot = get_spot_ohlcv(token, start_time_str, end_time_str)
        # df_perp, exchange_perp, symbol_perp = get_perp_ohlcv(token, start_time_str, end_time_str)
        '''spot price history'''
        symbol = f"{token}/USDT"
        df_spot = None
        exchange_spot = None
        for exchange in exchanges:
            try:
                df_spot = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
                exchange_spot = exchange
                if len(df_spot) == 0:
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
                symbol_proposal = [f"{multiplier}{token}", f"{token}{multiplier}",
                                   token] if token in tokens_multiplier_list else [token]
                break
        symbol_proposal = [f'{token}/USDT:USDT' for token in symbol_proposal]
        for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
            try:
                df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
                exchange_perp = exchange
                symbol_perp = symbol
                if len(df_perp) == 0:
                    df_perp = None
                    continue
                break
            except:
                print(f"No {symbol} in {exchange}...")
                continue
        volume_spot = (df_spot["volume"] * df_spot["close"]).sum()
        volume_perp = (df_perp["volume"] * df_perp["close"]).sum()

        if df_spot is None or df_perp is None:
            continue
        res.append([token, volume_spot, volume_perp, exchange_spot, exchange_perp])
        df = pd.DataFrame(res, columns=["token", "spot_volume_24h", "perp_volume_24h", "exchange_spot", "exchange_perp"])
        df.to_excel(f"Volume_{start_time_str}_{end_time_str}.xlsx", index=False)

if __name__ == '__main__':
    save_volume()