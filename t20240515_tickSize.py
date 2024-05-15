from datetime import datetime, timedelta

import requests
import pandas as pd

from crawl_common import get_ohlcv_df
from param import tokens_list, tokens_multiplier_dict

'''exp setting'''
tokens = tokens_list
end_time_str = '2024-05-14'
time_winodw = 1
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_winodw)
start_time_str = start_time_str.strftime("%Y-%m-%d")        # '2024-05-01'
exchanges = ['bybit', "binance", "okx"]
timeframe = '1h'
limit = 1000

def get_bybit_instruments() -> pd.DataFrame:
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {
        "category": "linear"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        data_list = data['result']['list']
        df = pd.DataFrame(data_list, columns=['symbol', 'status', 'baseCoin', 'priceScale'])

        df['tickSize'] = [item['priceFilter']['tickSize'] for item in data_list]
        df['minNotionalValue'] = [item['lotSizeFilter']['minNotionalValue'] for item in data_list]
        df['upperFundingRate'] = [item['upperFundingRate'] for item in data_list]
        df['lowerFundingRate'] = [item['lowerFundingRate'] for item in data_list]

        df['priceScale'] = df['priceScale'].astype(int)
        df['tickSize'] = df['tickSize'].astype(float)
        df['minNotionalValue'] = df['minNotionalValue'].astype(float)
        df['upperFundingRate'] = df['upperFundingRate'].astype(float)
        df['lowerFundingRate'] = df['lowerFundingRate'].astype(float)

        return df
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def save_tick_price():
    instruments_info = get_bybit_instruments()
    res = []
    for token in tokens:
        print(f"Now {token}: {tokens.index(token) + 1} of {len(tokens)}")

        exchange_perp, symbol_perp = None, None
        tickSize = None
        price = None
        symbol_proposal = [token]
        for multiplier, tokens_multiplier_list in tokens_multiplier_dict.items():
            if token in tokens_multiplier_list:
                symbol_proposal = [f"{multiplier}{token}", f"{token}{multiplier}", token] if token in tokens_multiplier_list else [token]
                break
        symbol_proposal = [f'{token}/USDT:USDT' for token in symbol_proposal]
        for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
            try:
                df_perp = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
                price = df_perp["close"].values[-1]
                isymbol = symbol.split(":")[0].replace(f"/", "")
                tickSize = instruments_info.loc[instruments_info['symbol'] == isymbol, "tickSize"].values[0]
                exchange_perp = exchange
                symbol_perp = symbol
                break
            except:
                print(f"No {symbol} in {exchange}...")
                continue

        if price is None:
            continue
        res.append([token, tickSize, price, exchange_perp])
        df = pd.DataFrame(res, columns=["token", "tickSize", "price", "exchange"])
        df.to_excel(f"TickSize_{start_time_str}_{end_time_str}.xlsx", index=False)

if __name__ == '__main__':
    save_tick_price()