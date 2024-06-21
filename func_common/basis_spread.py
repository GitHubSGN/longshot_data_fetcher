import re
import pandas as pd


def cal_basis_spread(df_spot: pd.DataFrame, df_perp: pd.DataFrame, symbol_perp: str):
    '''average spread &. last spread'''
    if df_spot is None or df_perp is None:
        return None, None
    else:
        price_col = "open"
        matches = re.findall(r"100*", symbol_perp.split(f"/")[0])
        if len(matches) > 0:
            multiplier = int(matches[0])
            df_spot[['open', 'high', 'low', 'close']] = df_spot[['open', 'high', 'low', 'close']] * multiplier
            df_spot["volume"] = df_spot["volume"] / multiplier
        df = pd.merge(df_spot[["open_time", price_col]], df_perp[["open_time", price_col]], on="open_time", how="inner",
                      suffixes=["Spot", "Perps"])
        df["spread"] = (df[f"{price_col}Spot"] - df[f"{price_col}Perps"]) / (df[f"{price_col}Perps"]) * 10000

        return df["spread"].mean(), df["spread"].values[-1], df["spread"].values[0], df