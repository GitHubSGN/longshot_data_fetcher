import copy
import os
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from tools.dir_util import project_dir

factor_horizon = 7
prediction_horizon = 21


def top_percent_columns(row, percent_unit, top_rank):
    lower_threshold = np.percentile(row, 100 - percent_unit * top_rank)
    upper_threshold = np.percentile(row, 100 - percent_unit * top_rank + percent_unit)

    # print(lower_threshold, upper_threshold)

    idx = (row > lower_threshold) & (row <= upper_threshold)
    return idx

def ranking_bst():
    xlsx_fn = os.path.join(project_dir(), "data", "a_funding", "FundingRate-90tokens_2023-01-01_2024-06-17.xlsx")
    df = pd.read_excel(xlsx_fn, index_col=0)
    df.fillna(0, inplace=True)

    df_factor = copy.deepcopy(df)
    df_pred = copy.deepcopy(df)
    for col in df.columns:
        df_factor[col] = df[col].rolling(window=factor_horizon * 3).sum()
        df_pred[col] = df[col].rolling(window=prediction_horizon * 3).sum().shift( -(prediction_horizon * 3 - 1) )

    df_factor = df_factor.iloc[factor_horizon * 3 - 1:-(prediction_horizon * 3 - 1), :]
    df_pred = df_pred.iloc[factor_horizon * 3 - 1:-(prediction_horizon * 3 - 1), :]

    res_df = None
    for tr in range(1,11):
        idx = df_factor.apply(lambda x: top_percent_columns(x, 10, tr), axis=1)
        symbols_sum = idx.sum(axis=1)
        weight_seq = 1.0 / symbols_sum
        weight_seq[symbols_sum==0] = 0
        weight_expanded = pd.DataFrame(np.zeros(df_factor.shape), index=df_factor.index, columns=df_factor.columns)
        for i in range(len(weight_seq)):
            weight_expanded.iloc[i, :] = weight_seq[i]
        weights = pd.DataFrame(np.zeros_like(df_factor), index=df_factor.index, columns=df_factor.columns)
        weights[idx] = weight_expanded[idx]

        pnl = (weights * df_pred).sum(axis=1)
        pnl.name = f"top{tr}"
        res_df = pnl.copy() if res_df is None else pd.concat([res_df, pnl], axis=1)
        print(f"top{tr}: {pnl.sum() / prediction_horizon / 3}")

    res_df.to_excel("ranking_bst_pnl.xlsx", index=True)
    res_df.index = [datetime.strptime(i, "%Y-%m-%d %H:%M:%S") for i in res_df.index]

    cum_pnl_df = res_df.cumsum() / prediction_horizon / 3
    cum_pnl_df.plot()
    plt.show()


    print("Done")


if __name__ == '__main__':
    ranking_bst()