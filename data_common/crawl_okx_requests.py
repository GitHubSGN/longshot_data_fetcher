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
        df = pd.DataFrame(data_list, columns=["uly", "state", "ctValCcy"])
        df.rename(columns = {"uly":'symbol', "state":'status', "ctValCcy":'baseCoin'}, inplace=True)

        df['tickSize'] = [item['tickSz'] for item in data_list]

        df['tickSize'] = df['tickSize'].astype(float)

        return df
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == '__main__':
    df = get_okx_instruments()