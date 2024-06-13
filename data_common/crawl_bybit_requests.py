import pandas as pd
import requests

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

def get_bybit_open_interest(symbol, category, start_time=None, end_time=None):
    base_url = "https://api.bybit.com/v5/market/open-interest"
    params = {
        "category": category,
        "symbol": symbol,
        "intervalTime": "1h",
        "limit": 200
    }

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    all_data = []
    while True:
        response = requests.get(base_url, params=params)
        data = response.json()

        if data["retCode"] != 0:
            raise Exception(f"API returned error: {data['retMsg']}")

        result = data["result"]
        all_data.extend(result["list"])

        if "nextPageCursor" not in result or not result["nextPageCursor"]:
            break

        params["cursor"] = result["nextPageCursor"]

    df = pd.DataFrame(all_data)
    df["symbol"] = symbol
    df["category"] = category
    df["oi"] = df["openInterest"].astype(float)
    df["timestamp"] = pd.to_numeric(df["timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    df = df[["symbol", "category", "oi", "timestamp"]]

    return df

def get_bybit_funding_rate(symbol, category, start_time=None, end_time=None):
    base_url = "https://api.bybit.com/v5/market/funding/history"
    params = {
        "category": category,
        "symbol": symbol,
        "limit": 200
    }

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    all_data = []
    while True:
        response = requests.get(base_url, params=params)
        data = response.json()

        if data["retCode"] != 0:
            raise Exception(f"API returned error: {data['retMsg']}")

        result = data["result"]
        list_data = result["list"]
        if not list_data:
            break

        all_data.extend(list_data)

        end_time = list_data[-1]["fundingRateTimestamp"]
        params["endTime"] = end_time

        if len(list_data) < 200:
            break

    df = pd.DataFrame(all_data)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["fundingRateTimestamp"] = pd.to_numeric(df["fundingRateTimestamp"])
    df["fundingRateTimestamp"] = pd.to_datetime(df["fundingRateTimestamp"], unit='ms')
    df = df[["symbol", "fundingRate", "fundingRateTimestamp"]]

    # 排除重复的 fundingRateTimestamp
    df = df.drop_duplicates(subset=["fundingRateTimestamp"])

    return df

if __name__ == '__main__':
    df_ins = get_bybit_instruments()

    symbol = "1000PEPEUSDT"
    category = "linear"
    start_time = 1715385600000 # 1711900800000
    end_time = 1715925600000

    df = get_bybit_open_interest(symbol, category, start_time, end_time)
    print(df)