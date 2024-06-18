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

def ranking_bst():
    xlsx_fn = os.path.join(project_dir(), "data", "a_funding", "FundingRate-90tokens_2023-01-01_2024-06-17.xlsx")
    df = pd.read_excel(xlsx_fn, index_col=0)
    # df.fillna(0, inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    df_factor = df.rolling(window=factor_horizon * 3).mean()

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
    ranking_bst()