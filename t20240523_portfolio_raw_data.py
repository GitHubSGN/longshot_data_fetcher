import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_common.crawl_bybit_requests import get_bybit_instruments
from data_common.crawl_common import get_ohlcv_df
from func_common.basis_spread import cal_basis_spread
from param import tokens_list, tokens_multiplier_dict

'''exp setting'''
tokens = tokens_list + ["WIF", "TAO"]
end_time_str = '2024-05-29 1:00:00'     # UTC Time Zone
time_winodw = 7
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d %H:%M:%S")
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def cal_raw_data(token: str, instruments_info: pd.DataFrame = None):
    """
    Prepare Raw Data, including:
        Basis: last basis, average basis
        Volatility: vol p.a.
        Tick: tick size
        Price: perps last price
        Volume: spot volume of last 24h, perps volume of last 24h
    """
    print(f"Now {token}: {tokens.index(token)+1} of {len(tokens)}")
    if instruments_info is None:
        instruments_info = get_bybit_instruments()

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
    tickSize = None
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
            isymbol = symbol.split(":")[0].replace(f"/", "")
            tickSize = instruments_info.loc[instruments_info['symbol'] == isymbol, "tickSize"].values[0]
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
        return tuple([None] * 10)
    else:
        print(f"df_spot shape: {df_spot.shape}, df_perp shape: {df_perp.shape}")
        print(f" df_spot / df_perp last open time: {df_spot['open_time'].values[-1]} / {df_perp['open_time'].values[-1]}.")
        ms, ls, _, df_spread = cal_basis_spread(df_spot, df_perp, symbol_perp)
        ss = df_spread["spread"].std()
        ms1d = df_spread["spread"][-24:].mean()
        mes1d = df_spread["spread"][-24:].median()
        mes7d = df_spread["spread"].median()
        volatility = np.log(df_spot["close"]).diff().std() * np.sqrt(pd.to_timedelta("365d") / pd.to_timedelta(timeframe))
        perp_price = df_perp["close"].values[-1]
        volume_spot = (df_spot["volume"].iloc[-24:] * df_spot["close"].iloc[-24:]).sum()
        volume_perp = (df_perp["volume"].iloc[-24:] * df_perp["close"].iloc[-24:]).sum()
        return ls, ms, ms1d, mes7d, mes1d, ss, volatility, tickSize, perp_price, volume_spot, volume_perp, exchange_spot, exchange_perp, symbol_perp

def cal_save_long_short_spread():
    res = []
    instruments_info = get_bybit_instruments()
    for token in tokens:
    # for token in ["RNDR", "ONDO", "MANA"]:
        ls, ms, ms1d, mes7d, mes1d, ss, volatility, tickSize, perp_price, volume_spot, volume_perp, exchange_spot, exchange_perp, symbol_perp =\
            cal_raw_data(token,instruments_info)
        if ms is None:
            continue
        # res.append([token, ls, ms, ms1d, ss, volatility, tickSize, perp_price, volume_spot, volume_perp, exchange_spot, exchange_perp, symbol_perp])
        # df = pd.DataFrame(res, columns=["token", "Bs", "MeanBs", "MeanBs1d", "StdBs", "vol(p.a.)", "tickSize", "perp_price", "spot_volume_24h", "perp_volume_24h", "exchange_spot", "exchange_perp", "symbol_perp"])
        res.append([token, ls, ms, ms1d, mes7d, mes1d, ss, volatility, tickSize, perp_price, volume_spot, volume_perp, exchange_spot, exchange_perp, symbol_perp])
        df = pd.DataFrame(res, columns=["token", "Bs", "MeanBs", "MeanBs1d", "MedianBs", "MedianBs1d", "StdBs", "vol(p.a.)", "tickSize", "perp_price", "spot_volume_24h", "perp_volume_24h", "exchange_spot", "exchange_perp", "symbol_perp"])
        df.to_excel(f"Raw_{start_time_str[:10]}_{end_time_str[:10]}.xlsx", index=False)

if __name__ == "__main__":
    cal_save_long_short_spread()
