import pandas as pd
import requests

def get_okx_instruments() -> pd.DataFrame:
    url = "https://okx.com/api/v5/public/instruments"
    params = {
        "ctType": "linear",
        "instType": "SWAP"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        data_list = data['data']
        df = pd.DataFrame(data_list, columns=["uly", "state", "settleCcy"])
        df.rename(columns = {"uly":'symbol', "state":'status', "settleCcy":'baseCoin'}, inplace=True)

        df['tickSize'] = [item['tickSz'] for item in data_list]

        df['tickSize'] = df['tickSize'].astype(float)

        return df
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_okx_funding_rate(symbol, category=None, start_time=None, end_time=None):
    base_url = "https://okx.com/api/v5/public/funding-rate-history"
    params = {
        "instId": symbol,
        "limit": 100
    }

    if start_time:
        params["before"] = start_time
    if end_time:
        params["after"] = end_time

    all_data = []
    while True:
        response = requests.get(base_url, params=params)
        data = response.json()

        if data["code"] != '0':
            raise Exception(f"API returned error: {data['retMsg']}")

        list_data = data["data"]
        if not list_data:
            break

        all_data.extend(list_data)

        end_time = list_data[-1]["fundingTime"]
        params["after"] = end_time

        if len(list_data) < 100:
            break

    df = pd.DataFrame(all_data)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["fundingRateTimestamp"] = pd.to_numeric(df["fundingTime"])
    df["fundingRateTimestamp"] = pd.to_datetime(df["fundingRateTimestamp"], unit='ms')
    df.rename(columns={"instId": "symbol"}, inplace=True)
    df = df[["symbol", "fundingRate", "fundingRateTimestamp"]]

    # 排除重复的 fundingRateTimestamp
    df = df.drop_duplicates(subset=["fundingRateTimestamp"])

    return df

if __name__ == '__main__':
    df = get_okx_instruments()

    symbol = "BTC-USDT-SWAP"
    category = "as"
    start_time = 1715385600000
    start_time = 1711900800000
    end_time = 1715925600000

    df_fr = get_okx_funding_rate(symbol, category, start_time, end_time)
    print(df_fr)