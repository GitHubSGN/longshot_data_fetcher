import time
import pandas as pd
import numpy as np
from research.data.histbarreader import HistBarReader
from research.factoranalysis_OLS.factor_ols import FactorOLS
from research.supporting_func.supportingfunctions import print_stat
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

if __name__ == '__main__':
    pd.options.display.width = 0
    symbols = ['1000SHIB', 'ALICE', 'ANKR', 'AR', 'ARPA', 'ATA', 'AUDIO', 'AXS', 'BAKE', 'BAND', 'BLZ', 'C98', 'CHZ', 'CTK', 'DENT', 'DOGE', 'DUSK', 'ENJ', 'FIL', 'GALA', 'GRT', 'GTC', 'LINK', 'LIT', 'LPT', 'MANA', 'MASK', 'MTL', 'NKN', 'OCEAN', 'OGN', 'ONT', 'RLC', 'SAND', 'SFP', 'STORJ', 'THETA', 'TRB', 'ZEC', 'ZEN']
    # ['BTC', 'ETH', 'MATIC', '1000SHIB', 'MANA']
    base_currency = 'USDT'
    exchange = 'BINANCE_USDT'
    debug = False
    numberofdays = 1500

    start = time.time()
    raw_data = dict()
    for symbol in symbols:
        histbarreader = HistBarReader(pair_name=symbol, number_of_date=numberofdays, base_currency=base_currency,
                                      exchange=exchange, debug=debug)
        histbarreader.readdata()
        tempdata = histbarreader.getdata()
        raw_data[symbol] = tempdata
    raw_close_df = pd.DataFrame({s: bars['close'] for s, bars in raw_data.items()})
    raw_close_df.index = pd.DatetimeIndex(raw_close_df.index)

    raw_data_spot = dict()
    exchange = 'BINANCE_SPOT'
    spot_future_map = {x.replace('1000', ''): x for x in symbols}
    symbols_spot = list(spot_future_map.keys())  # [x.replace('1000', '') for x in symbols]
    for symbol in symbols_spot:
        histbarreader = HistBarReader(pair_name=symbol, number_of_date=numberofdays, base_currency=base_currency,
                                      exchange=exchange, debug=debug)
        histbarreader.readdata()
        tempdata = histbarreader.getdata().copy()
        if '1000' in spot_future_map[symbol]:
            tempdata.loc[:, ['close', 'high', 'low', 'open']] = tempdata.loc[:, ['close', 'high', 'low', 'open']] * 1000
        raw_data_spot[spot_future_map[symbol]] = tempdata
    endofpredict = time.time()
    print('data reading takes: ' + str(endofpredict - start))

    freq_set = ['6h', '3h']
    half_life_set = range(25, 200, 25)
    prediction_shift = 2
    portfolio_limit = 2.5
    asset_limit = 1
    voltarget = 0.15
    factorols = FactorOLS(freq_set=freq_set, half_life_set=half_life_set, prediction_shift=prediction_shift,
                          portfolio_limit=portfolio_limit, asset_limit=asset_limit, voltarget=voltarget, verbose=False)
    factorols.setdata(raw_data=raw_data, raw_data_spot=raw_data_spot)
    temp_expression = "calc_aroon(dict_df, half_life)"  # 'series_hlnormalization(calc_mfi(dict_df, half_life), half_life)'   'mytest(dict_df, half_life)'
    factorols.predict(temp_expression)
    momsignal = factorols.get_allocation()['TS']
    momsignal_xs = factorols.get_allocation()['XS']
    assetreturn = np.log(raw_close_df.loc[momsignal.index.intersection(raw_close_df.index), :]).diff()
    momsignal.to_csv('factorols_TS_pos.csv')
    momsignal_xs.to_csv('factorols_XS_pos.csv')

    signal_shift = 1
    tempsignal = momsignal.shift(signal_shift)
    anualized_factor = 365.0 * pd.to_timedelta('24hours') / pd.to_timedelta(freq_set[-1])
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=True,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=False,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    (tempsignal * assetreturn).sum(axis=1).cumsum().to_frame().plot()
    plt.show(block=False)

    tempsignal_xs = momsignal_xs.shift(signal_shift)
    anualized_factor = 365.0 * pd.to_timedelta('24hours') / pd.to_timedelta(freq_set[-1])
    print_stat(tempsignal=tempsignal_xs, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=True,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    print_stat(tempsignal=tempsignal_xs, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=False,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    (tempsignal_xs * assetreturn).sum(axis=1).cumsum().to_frame().plot()
    plt.show(block=True)
