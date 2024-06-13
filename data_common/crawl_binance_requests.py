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

if __name__ == '__main__':
    df = get_binance_instruments()
