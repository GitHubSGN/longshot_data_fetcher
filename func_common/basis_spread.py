import re
import pandas as pd


def cal_basis_spread(df_spot: pd.DataFrame, df_perp: pd.DataFrame, symbol_perp: str):
    '''average spread &. last spread'''
    if df_spot is None or df_perp is None:
        return None, None
    else:
        matches = re.findall(r"100*", symbol_perp.split(f"/")[0])
        if len(matches) > 0:
            multiplier = int(matches[0])
            df_spot[['open', 'high', 'low', 'close']] = df_spot[['open', 'high', 'low', 'close']] * multiplier
            df_spot["volume"] = df_spot["volume"] / multiplier
        df = pd.merge(df_spot[["open_time", "close"]], df_perp[["open_time", "close"]], on="open_time", how="inner",
                      suffixes=["Spot", "Perps"])
        df["spread"] = (df["closeSpot"] - df["closePerps"]) / (df["closePerps"]) * 10000
        return df["spread"].mean(), df["spread"].values[-1], df["spread"].values[0]