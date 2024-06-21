import copy
import os
import warnings
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from tools.dir_util import project_dir
from tools.timer_utils import timer

warnings.filterwarnings('ignore', category=RuntimeWarning, message='All-NaN slice encountered')

COMISSION = 4.6 * 2 / 1e4
SLIPPAGE = 10 / 1e4

def top_percent_columns(row, percent_unit, top_rank):
    lower_threshold = np.nanpercentile(row, 100 - percent_unit * top_rank)
    upper_threshold = np.nanpercentile(row, 100 - percent_unit * top_rank + percent_unit)
    # print(lower_threshold, upper_threshold)
    idx = (row >= lower_threshold) & (row <= upper_threshold)
    return idx / sum(idx)

def deal_data():
    wd = os.path.join(project_dir(), "data", "a_funding", "2023-01-01-2024-06-19")
    df_perp = None
    df_spot = None
    for dirpath, dirnames, filenames in os.walk(wd):
        print(f'Found directory: {dirpath}')
        for filename in filenames:
            print(filename)
            temp_file = os.path.join(wd, filename)
            df = pd.read_excel(temp_file, index_col=0)
            df.index = pd.DatetimeIndex(df.index)
            df.index.name = None
            df.index = df.index - pd.to_timedelta('8h') ### adjust for UTC
            ttt = df.open ### use open data because the time index is open time
            ttt.name = filename.split('_')[0]
            if 'perp' in filename:
                df_perp = pd.concat([df_perp, ttt], axis=1)
            if 'spot' in filename:
                df_spot = pd.concat([df_spot, ttt], axis=1)
    df_perp.to_csv(os.path.join(project_dir(), "data", "a_funding", "bybit", 'df_perp.csv'))
    df_spot.to_csv(os.path.join(project_dir(), "data", "a_funding", "bybit", 'df_spot.csv'))

def cal_sharp_ratio(pnl):
    ret_ap_avg = pnl.mean() * (pd.to_timedelta('365d') / (pnl.index[-1] - pnl.index[-2]))
    ret_ap_std = pnl.std() * np.sqrt(pd.to_timedelta('365d') / (pnl.index[-1] - pnl.index[-2]))
    ret_sharp = pnl.mean() / pnl.std() * np.sqrt(pd.to_timedelta('365d') / (pnl.index[-1] - pnl.index[-2]))
    return ret_ap_avg, ret_ap_std, ret_sharp

def ranking_strategy_bst(funding_df, return_df, start_shift = 1, factor_horizon=7, prediction_horizon=21, percent_unit=10,
                         top_bunches = 100, baseline=None, show=False, print_log=False, save=False):
    df_factor = funding_df.rolling(window=factor_horizon * 3).mean()
    # df_factor = funding_df.rolling(window=factor_horizon * 3).mean() - 1 * funding_df.rolling(window=factor_horizon * 3).std()
    # df_factor = funding_df.rolling(window=factor_horizon * 3).mean() * 3 * 365 - 1 * funding_df.rolling(window=factor_horizon * 3).std() * np.sqrt(3*365)

    pnl_df = None
    tc_df = None
    turnover_list = []
    for tr in range(1, min([int(100/percent_unit)+1, top_bunches+1])):
        if print_log:
            print(f"top{tr}:")
        signal = df_factor.apply(lambda x: top_percent_columns(x, percent_unit, tr), axis=1)
        # (signal.shift(1) * df).sum(axis=1).cumsum().plot()
        resampled_signal = signal.tail(-start_shift).resample(f'{prediction_horizon}d', offset=f'{prediction_horizon}d',
                                                              label='right', closed='right', origin='start').last()
        resampled_signal = resampled_signal.reindex(return_df.index).ffill()

        turnover = np.abs(resampled_signal.diff()).sum(axis=1)
        turnover_list.append(turnover.sum())
        if print_log:
            print(f"Avg. turnover per day: {sum(turnover) / ((resampled_signal.index[-1] - resampled_signal.index[0]) / pd.to_timedelta('1d'))}")
            print(f"Avg. return per trade: {(resampled_signal.shift(1) * return_df).sum(axis=1).sum() / sum(turnover)}")

        # (resampled_signal.shift(1) * df).sum(axis=1).cumsum().plot()
        # plt.show()
        pnl = (resampled_signal.shift(1) * return_df).sum(axis=1)
        tc = (resampled_signal.diff().abs() * (COMISSION + SLIPPAGE)).sum(axis=1)

        pnl.name = f"top{tr}"
        pnl_df = pd.concat([pnl_df, pnl], axis=1)
        tc.name = f"top{tr}"
        tc_df = pd.concat([tc_df, tc], axis=1)
        if print_log:
            print(f"cum pnl: {pnl.sum()}, cum tc: {tc.sum()} => cum return: {pnl.sum() - tc.sum()}")

    # add baseline
    if baseline is None:
        baseline = []
    for token in baseline:
        pnl = return_df[token]
        pnl_df = pd.concat([pnl_df, pnl], axis=1)
        tc = pd.Series(0, index=pnl.index, name=pnl.name)
        tc_df = pd.concat([tc_df, tc], axis=1)

    if save:
        pnl_df.to_excel(f"ranking_bst_pnl-{start_shift}-{factor_horizon}-{prediction_horizon}-{percent_unit}.xlsx", index=True)
    pnl_df.index = return_df.index
    if show:
        pnl_df.cumsum().plot(title = "Cum. Funding Rate Ret.")
        plt.legend(loc='upper left')
        plt.show()
        (pnl_df.cumsum() - tc_df.cumsum()).plot(title="Cum. Funding Rate Ret. (with TC)")
        plt.legend(loc='upper left')
        plt.show()
    return pnl_df, tc_df, turnover_list


def ranking_bst(exchange = "bybit"):
    if exchange == "bybit":
        # with timer("read xlsx"):
        #     xlsx_fn = os.path.join(project_dir(), "data", "a_funding", exchange, "FundingRate-90tokens_2023-01-01_2024-06-17.xlsx")
        #     df = pd.read_excel(xlsx_fn, index_col=0)
        with timer("read csv"):
            csv_fn = os.path.join(project_dir(), "data", "a_funding", exchange, "FundingRate-90tokens_2023-01-01_2024-06-17.csv")
            df = pd.read_csv(csv_fn, index_col=0)
        # df.fillna(0, inplace=True)
        df.index = pd.DatetimeIndex(df.index)

        df_spot = pd.read_csv(os.path.join(project_dir(), "data", "a_funding", exchange, 'df_spot.csv'))
        df_perp = pd.read_csv(os.path.join(project_dir(), "data", "a_funding", exchange, 'df_perp.csv'))
        df_spot.index = pd.DatetimeIndex(df_spot.iloc[:, 0])
        df_perp.index = pd.DatetimeIndex(df_perp.iloc[:, 0])
        df_spot.index.name = None
        df_perp.index.name = None

        df_spot = df_spot.drop(df_spot.columns[[0]], axis=1)
        df_perp = df_perp.drop(df_perp.columns[[0]], axis=1)

        df_spot = df_spot.reindex(df.index)
        df_perp = df_perp.reindex(df.index)

        ccc = list(set(df.columns).intersection(set(df_perp.columns)).intersection(set(df_spot.columns)))
        ccc.sort()

        df_spot = df_spot.loc[:,ccc]
        df_perp = df_perp.loc[:, ccc]
        df = df.loc[:, ccc]

        ### long spot short perp
        pnl_spread = np.log(df_spot).diff() - np.log(df_perp).diff()
        return_df = pnl_spread + df

        case_study = False
        if case_study:
            pnl_df, tc_df, turnover_list = ranking_strategy_bst(df, df, start_shift=3, factor_horizon=7, prediction_horizon=21, percent_unit=10, top_bunches=10, baseline=["BTC", "ETH"], show=True, print_log=True)
            # pnl_df, tc_df, turnover_list = ranking_strategy_bst(df, df, start_shift=3, factor_horizon=7, prediction_horizon=1, percent_unit=10, top_bunches=10, baseline=["BTC", "ETH"], show=True, print_log=True)
            ret_ap_avg, ret_ap_std, ret_sharp = cal_sharp_ratio(pnl_df)
            display_cols = [f"top{i}" for i in range(1,4)] + ["BTC", "ETH"]
            for col in display_cols:
                ravg, rstd, rshp = ret_ap_avg[col], ret_ap_std[col], ret_sharp[col]
                print(f"[W.O. TC, {col}] Avg. Ret(a.p.): {ravg:.2%}, Std. Ret(a.p.): {rstd:.2%}, Sharp Ratio: {rshp:.2f}")
            ret_ap_avg, ret_ap_std, ret_sharp = cal_sharp_ratio(pnl_df - tc_df)
            for col in display_cols:
                ravg, rstd, rshp = ret_ap_avg[col], ret_ap_std[col], ret_sharp[col]
                print(f"[With TC, {col}] Avg. Ret(a.p.): {ravg:.2%}, Std. Ret(a.p.): {rstd:.2%}, Sharp Ratio: {rshp:.2f}")

        res = []
        xlsx_pos = datetime.now().strftime('%y%m%d-%H%M%S')
        for fh in range(1, 31):
            for ph in range(1, 31):
                pnl_df, tc_df, turnover_list = ranking_strategy_bst(df, df, start_shift=3, factor_horizon=fh, prediction_horizon=ph, percent_unit=10, top_bunches=1, show=False, print_log=False)
                print(fh, ph, pnl_df.sum().values[0], turnover_list[0])
                # res.append([fh, ph, pnl_df.sum(), turnover_list[0]])
                # res_df = pd.DataFrame(res, columns=["factor_horizon", "prediction_horizon", "cum_pnl", "turnover"])

                cur_res = [fh, ph, turnover_list[0]]
                cols = ["factor_horizon", "prediction_horizon", "turnover"]
                for idx, tc_mul in enumerate( [0, (4.6*2+10)/1e4, (2*2+10)/1e4] ):
                    cpnl_df = pnl_df - tc_df * tc_mul / (COMISSION + SLIPPAGE)
                    ret_ap_avg, ret_ap_std, ret_sharp = cal_sharp_ratio(cpnl_df)
                    ravg, rstd, rshp = ret_ap_avg["top1"], ret_ap_std["top1"], ret_sharp["top1"]
                    cur_res.extend( [cpnl_df.sum().values[0], ravg, rstd, rshp] )
                    cols.extend( [f"{idx}_{c}" for c in ["cumRet", "avgRet", "stdRet", "Sharp"]])

                res.append(cur_res)
                res_df = pd.DataFrame(res, columns=cols)
                res_df.to_excel(f"tranverse_{xlsx_pos}.xlsx")

    print("Done")


if __name__ == '__main__':
    ### deal_data()
    ranking_bst()