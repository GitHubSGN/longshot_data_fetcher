import os
import pandas as pd

from data_common.data_api import get_funding_rate
from tools.dir_util import dir_exp_sday, create_directory

tokens = ["PEPE", "ARB", "FLOKI", "MANTA", "BONK", "BTC", "ETH"]
windows = [1, 3, 7, 14, 21, 30]

def cal_funding_corr(token: str):
    df = get_funding_rate(token)
    f_cols = []
    p_cols = []
    for w in [int(w*3) for w in windows]:
        c1 = f"f_{int(w/3)}"
        c2 = f"p_{int(w/3)}"
        f_cols.append(c1)
        p_cols.append(c2)
        df[c1] = df["fundingRate"].rolling(w).sum() / int(w/3) * 365
        df[c2] = df[c1].shift(-w)

    # Initialize an empty DataFrame for the correlation matrix
    corr_matrix = pd.DataFrame(index=f_cols, columns=p_cols)

    # Calculate the correlation matrix
    for col1 in f_cols:
        for col2 in p_cols:
            corr_matrix.at[col1, col2] = df[col1].corr(df[col2])

    return corr_matrix

def cal_save_funding_corr():
    res_dir = os.path.join( dir_exp_sday("20240518"), "fundingCorr" )
    output_path = os.path.join( res_dir, 'funding_correlation_v0.xlsx' )
    create_directory(output_path)

    with pd.ExcelWriter(output_path) as writer:
        for token in tokens:
            print(f"Now {token}:{tokens.index(token) + 1} of {len(tokens)}")
            df = cal_funding_corr(token)
            df.to_excel(writer, sheet_name=token)
            print(df)

    print(f"Funding Correlations written to {output_path}")


if __name__ == '__main__':
    cal_save_funding_corr()