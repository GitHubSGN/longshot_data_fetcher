from datetime import datetime, timedelta
import pandas as pd

from data_common.crawl_bybit_requests import get_bybit_funding_rate
from data_common.data_api import get_spot_ohlcv, get_perp_ohlcv, perp_symbol_proposal
from param import tokens_list
from tools.date_util import datetime_to_timestamp_tz0

'''exp setting'''
tokens = ["BTC", "ETH", "SOL", "AVAX", "PEPE", "LINK", "ARB", "NEAR", "BONK", "DOGE", "ORDI", "ENS", "FIL", "LDO"]
end_time_str = '2024-06-04'
time_window = 30
start_time_str = datetime.strptime(end_time_str, "%Y-%m-%d") - timedelta(days = time_window)
start_time_str = start_time_str.strftime("%Y-%m-%d")

def save_funding_rate():
    start_time_ts = datetime_to_timestamp_tz0(datetime.strptime(start_time_str, "%Y-%m-%d"))
    end_time_ts = datetime_to_timestamp_tz0(datetime.strptime(end_time_str, "%Y-%m-%d"))
    res = {}
    for token in tokens:
        print(f"Now {token}: {tokens.index(token) + 1} of {len(tokens)}")

        exchanges = ["bybit"]
        symbol_proposal = perp_symbol_proposal(token)
        fr_list = None
        idx = None
        for (exchange, symbol) in [(exchange, symbol) for exchange in exchanges for symbol in symbol_proposal]:
            try:
                isymbol = symbol.split(":")[0].replace(f"/", "")
                df = get_bybit_funding_rate(isymbol, "linear", start_time=start_time_ts, end_time=end_time_ts)
                df.rename(columns={"fundingRateTimestamp": "open_time"}, inplace=True)
                df["open_time"] = df["open_time"].apply(lambda x: str(x))
                df["ts"] = df["open_time"].apply(lambda x: datetime_to_timestamp_tz0(datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
                df = df.iloc[::-1]
                fr_list = df["fundingRate"].to_list()
                idx = df["open_time"].to_list()
                if len(df) == 0:
                    continue
                break
            except:
                print(f"No {symbol} in {exchange}...")
                continue
        res[token] = fr_list
        df = pd.DataFrame(res)
        df.index = idx
        df.to_excel(f"FundingRate-{len(tokens)}tokens_{start_time_str}_{end_time_str}.xlsx")

if __name__ == '__main__':
    save_funding_rate()