import os
import warnings
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from tools.dir_util import project_dir

warnings.filterwarnings('ignore', category=RuntimeWarning, message='All-NaN slice encountered')

factor_horizon = 7
prediction_horizon = 21

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
    df_perp.to_csv(os.path.join(project_dir(), "data", "a_funding",'df_perp.csv'))
    df_spot.to_csv(os.path.join(project_dir(), "data", "a_funding",'df_spot.csv'))



def ranking_bst():
    xlsx_fn = os.path.join(project_dir(), "data", "a_funding", "FundingRate-90tokens_2023-01-01_2024-06-17.xlsx")
    df = pd.read_excel(xlsx_fn, index_col=0)
    # df.fillna(0, inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    df_factor = df.rolling(window=factor_horizon * 3).mean()

    df_spot = pd.read_csv(os.path.join(project_dir(), "data", "a_funding",'df_spot.csv'))
    df_perp = pd.read_csv(os.path.join(project_dir(), "data", "a_funding", 'df_perp.csv'))
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
    df = pnl_spread + df

    pnl_df = None
    for tr in range(1,11):
        print(f"top{tr}:")
        signal = df_factor.apply(lambda x: top_percent_columns(x, 10, tr), axis=1)
        # (signal.shift(1) * df).sum(axis=1).cumsum().plot()
        start_shift = 3
        resampled_signal = signal.tail(-start_shift).resample(f'{prediction_horizon}d', offset=f'{prediction_horizon}d', label='right', closed='right', origin = 'start').last()
        resampled_signal = resampled_signal.reindex(df.index).ffill()

        turnover = np.abs(resampled_signal.diff()).sum(axis=1)
        print( f"Avg. turnover per day: {sum(turnover) / ((resampled_signal.index[-1] - resampled_signal.index[0]) / pd.to_timedelta('1d'))}" )
        print( f"Avg. return per trade: {(resampled_signal.shift(1) * df).sum(axis=1).sum() / sum(turnover)}" )

        # (resampled_signal.shift(1) * df).sum(axis=1).cumsum().plot()
        # plt.show()
        pnl = (resampled_signal.shift(1) * df).sum(axis=1)
        pnl.name = f"top{tr}"
        pnl_df = pnl.copy() if pnl_df is None else pd.concat([pnl_df, pnl], axis=1)
        print(f"cum pnl: {pnl.sum()}")

    pnl_df.to_excel("ranking_bst_pnl.xlsx", index=True)
    pnl_df.index = df.index
    cum_pnl_df = pnl_df.cumsum()
    cum_pnl_df.plot()
    plt.show()

    print("Done")


if __name__ == '__main__':
    ### deal_data()
    ranking_bst()