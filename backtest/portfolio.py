from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from data_common.data_api import get_funding_rate, get_spot_ohlcv, get_perp_ohlcv, get_ticksize, get_open_interest
from func_common.basis_spread import cal_basis_spread
from param import tokens_list
from tools.timer_utils import timer


def portfolio_raw_data(current_date: str, factor_horizon: int = 14, predicted_horizon: int = 14):
    df_tick = get_ticksize()

    current_start_date = datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=factor_horizon)
    current_start_date = current_start_date.strftime("%Y-%m-%d")
    res = []
    cols = ["base", "factor horizon", "predicted horzion", "Funding_Rate(bps)", "Funding_Rate(p.a.)", "Basis_Spread(bps)", "Mean(Basis_Spread)",
            "spread cost", "tickSize", "price", "Transaction Cost", "net return", "net return(p.a.)", "spot volume 24h", "perp volume 24h",
            "open_interest_value", "Volatility(p.a.)"]
    for token in tokens_list: # ["PEPE", "ARB", "FLOKI", "MANTA"]:#
        print(f"{token}")
        with timer("read data"):        # 2.5s
            df_fr = get_funding_rate(token, start_str = current_start_date, end_str = current_date)
            df_spot, _ = get_spot_ohlcv(token, start_str = current_start_date, end_str = current_date)
            df_perp, _, symbol_perp = get_perp_ohlcv(token, start_str = current_start_date, end_str = current_date)
            df_oi = get_open_interest(token, start_str = current_start_date, end_str = current_date)
            if df_fr is None or df_spot is None or df_perp is None or df_oi is None or \
                    len(df_spot)==0 or len(df_perp)==0 or len(df_oi)==0 or len(df_fr)==0:
                continue

        with timer("cal indicators"):   # 0.008s
            fr = df_fr["fundingRate"].sum()
            fr_pa = fr/factor_horizon*365

            ms, ls, _, _ = cal_basis_spread(df_spot, df_perp, symbol_perp)

            ticksize = df_tick.loc[ df_tick["token"]==token, "tickSize" ].values[0]
            price = df_perp["close"].values[-1]
            tc = (5 + ticksize / price * 10000) * 4

            spot = df_spot.iloc[-24:,:]
            spot_volume = (spot["volume"] * spot["close"]).sum()
            perp = df_perp.iloc[-24:, :]
            perp_volume = (perp["volume"] * perp["close"]).sum()

            oi = df_oi["oi"].values[-1]
            vol = np.log(df_spot["close"]).diff().std() * np.sqrt( pd.to_timedelta("365d") / pd.to_timedelta("1h")  )

            net_return = fr_pa/365*predicted_horizon + (ms-ls)/10000 - tc / 10000
        res.append(
            [token, factor_horizon, predicted_horizon, fr*10000, fr_pa, ls, ms, ms-ls, ticksize, price, tc,
             net_return, net_return / predicted_horizon * 365,
             spot_volume, perp_volume, oi, vol
             ]
        )
    df = pd.DataFrame(res, columns=cols)

    return df

def get_realized_return(current_date: str, predicted_horizon: int = 14):
    df_tick = get_ticksize()

    current_start_date = current_date
    current_end_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=predicted_horizon)
    current_end_date = current_end_date.strftime("%Y-%m-%d")
    res = []
    cols = ["base", "predicted horzion", "Funding_Rate(bps)", "Funding_Rate(p.a.)",
            "Basis_Spread(bps)", "Last Basis_Spread",
            "spread cost", "tickSize", "price", "Transaction Cost", "net return", "net return(p.a.)"]
    for token in tokens_list:  # ["PEPE", "ARB", "FLOKI", "MANTA"]:#
        print(f"{token}")
        with timer("read data"):  # 2.5s
            df_fr = get_funding_rate(token, start_str=current_start_date, end_str=current_end_date)
            df_spot, _ = get_spot_ohlcv(token, start_str=current_start_date, end_str=current_end_date)
            df_perp, _, symbol_perp = get_perp_ohlcv(token, start_str=current_start_date, end_str=current_end_date)
            if df_fr is None or df_spot is None or df_perp is None  or \
                    len(df_spot)==0 or len(df_perp)==0 or len(df_fr)==0:
                continue

        with timer("cal indicators"):  # 0.008s
            fr = df_fr["fundingRate"].sum()
            fr_pa = fr / predicted_horizon * 365

            _, ls, fs, _ = cal_basis_spread(df_spot, df_perp, symbol_perp)

            ticksize = df_tick.loc[df_tick["token"] == token, "tickSize"].values[0]
            price = df_perp["close"].values[0]
            tc = (5 + ticksize / price * 10000) * 4

            net_return = fr_pa / 365 * predicted_horizon + (ls - fs) / 10000 - tc / 10000
        res.append(
            [token, predicted_horizon, fr * 10000, fr_pa, fs, ls, ls - fs, ticksize, price, tc,
             net_return, net_return / predicted_horizon * 365,
             ]
        )
    df = pd.DataFrame(res, columns=cols)

    return df


def construct_portfolio(current_date: str,
                        factor_horizon: int = 14, predicted_horizon: int = 14,
                        total_risk_budget: float = 1.8, init_risk_budge: float = 1.3,
                        max_allocation_pct: float = 0.1, max_oi_percent: float = 0.1):
    # universe
    token = tokens_list
    df_raw = portfolio_raw_data(current_date, factor_horizon, predicted_horizon)
    df_raw.to_excel("basis.xlsx", index=False)
    # print("hh")
    df_realized_return = get_realized_return(current_date, predicted_horizon = 14)
    df_realized_return.to_excel("realizedReturn.xlsx", index=False)

if __name__ == '__main__':
    # portfolio_raw_data("2024-05-14")
    construct_portfolio("2024-02-01")
