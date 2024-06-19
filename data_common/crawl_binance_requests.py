import pandas as pd
import requests

def get_binance_instruments() -> pd.DataFrame:
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        symbols = data["symbols"]

        df = pd.DataFrame(
            symbols,
            columns=[
                "symbol",
                "status",
                "baseAsset",
                "quoteAsset",
                "pricePrecision",
                "quantityPrecision",
            ],
        )

        df_filtered = df[df["status"] == "TRADING"]

        df_filtered["tick_Size"] = [
            float(item["filters"][0]["tickSize"]) for item in symbols if item["status"] == "TRADING"
        ]
        df_filtered["min_qty"] = [
            float(item["filters"][1]["minQty"]) for item in symbols if item["status"] == "TRADING"
        ]

        df_filtered.rename(
            columns={
                "baseAsset": "baseCoin",
                "quoteAsset": "quoteCoin",
                "pricePrecision": "priceScale",
                "quantityPrecision": "quantityScale",
                "tick_Size": "tickSize"
            },
            inplace=True,
        )

        return df_filtered

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_binance_funding_rate(symbol, category=None, start_time=None, end_time=None):
    base_url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {
        "symbol": symbol,
        "limit": 100
    }

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    all_data = []
    while True:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        list_data = response.json()
        if not list_data:
            break

        all_data.extend(list_data)

        end_time = list_data[-1]["fundingTime"]
        params["startTime"] = end_time

        if len(list_data) < 100:
            break

    df = pd.DataFrame(all_data)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["fundingRateTimestamp"] = pd.to_numeric(df["fundingTime"])
    df["fundingRateTimestamp"] = pd.to_datetime(df["fundingRateTimestamp"], unit='ms')
    df = df[["symbol", "fundingRate", "fundingRateTimestamp"]]

    # 排除重复的 fundingRateTimestamp
    df = df.drop_duplicates(subset=["fundingRateTimestamp"])

    return df

if __name__ == '__main__':
    df = get_binance_instruments()

    symbol = "BTCUSDT"
    category = "as"
    # start_time = 1715385600000
    start_time = 1711900800000
    end_time = 1715925600000

    df_fr = get_binance_funding_rate(symbol, category, start_time, end_time)
    print(df_fr)
