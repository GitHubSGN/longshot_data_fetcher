from statsmodels.regression.rolling import RollingOLS
import pandas as pd
import numpy as np
from statsmodels.tools import add_constant
from research.supporting_func.dataresampler import DataResampler
from research.supporting_func.supportingfunctions import signalround, XSnormalization, fundlevelnormalization, \
    risk_manage_portfolio, risk_manage_asset
from research.supporting_func.sist_dict import *
from typing import Dict


class FactorOLS:
    def __init__(self, freq_set, half_life_set, prediction_shift=2, portfolio_limit=2.5, asset_limit=1, voltarget=0.15,
                 verbose=False):
        self.raw_close_df = None
        self.symbols = None
        self.raw_data_spot = None
        self.raw_data = None
        self.allocation = None
        self.freq_set = freq_set
        self.half_life_set = half_life_set
        self.prediction_shift = prediction_shift
        self.normalize_lookbackperiod = int(pd.to_timedelta('30d') / pd.to_timedelta(self.freq_set[-1])) * 1
        self.voltarget = voltarget
        self.portfolio_limit = portfolio_limit
        self.asset_limit = asset_limit
        self.verbose = verbose

        self.close_df = None
        self.high_df = None
        self.low_df = None
        self.open_df = None
        self.volume_df = None
        self.close_df_spot = None
        self.high_df_spot = None
        self.low_df_spot = None
        self.open_df_spot = None
        self.volume_df_spot = None

    def get_allocation(self):
        return self.allocation

    def get_assetreturn(self):
        return self.assetreturn

    def setdata(self, raw_data: Dict[str, pd.DataFrame], raw_data_spot: Dict[str, pd.DataFrame]):
        assert set(raw_data.keys()) == set(raw_data_spot.keys())
        self.raw_data = raw_data
        self.raw_data_spot = raw_data_spot
        self.symbols = self.raw_data.keys()
        self.raw_close_df = pd.DataFrame({s: bars['close'] for s, bars in self.raw_data.items()})
        self.raw_close_df.index = pd.DatetimeIndex(self.raw_close_df.index)

    def setdata_freq(self, freq):
        port_bars = dict()
        port_bars_spot = dict()
        for symbol in self.symbols:
            tempdata = self.raw_data[symbol]
            tempdata_spot = self.raw_data_spot[symbol]
            dataresample = DataResampler(freq)
            resampled_tempdata = dataresample.dataresampler(tempdata)
            resampled_tempdata_spot = dataresample.dataresampler(tempdata_spot)
            port_bars[symbol] = resampled_tempdata
            port_bars_spot[symbol] = resampled_tempdata_spot

        self.close_df = pd.DataFrame({s: bars['close'] for s, bars in port_bars.items()}).ffill()
        self.high_df = pd.DataFrame({s: bars['high'] for s, bars in port_bars.items()}).ffill()
        self.low_df = pd.DataFrame({s: bars['low'] for s, bars in port_bars.items()}).ffill()
        self.open_df = pd.DataFrame({s: bars['open'] for s, bars in port_bars.items()}).ffill()
        self.volume_df = pd.DataFrame({s: bars['volume'] for s, bars in port_bars.items()}).ffill()

        self.close_df_spot = pd.DataFrame({s: bars['close'] for s, bars in port_bars_spot.items()}).ffill()
        self.high_df_spot = pd.DataFrame({s: bars['high'] for s, bars in port_bars_spot.items()}).ffill()
        self.low_df_spot = pd.DataFrame({s: bars['low'] for s, bars in port_bars_spot.items()}).ffill()
        self.open_df_spot = pd.DataFrame({s: bars['open'] for s, bars in port_bars_spot.items()}).ffill()
        self.volume_df_spot = pd.DataFrame({s: bars['volume'] for s, bars in port_bars_spot.items()}).ffill()

    def predict(self, temp_expression):
        pos_df_ = pd.DataFrame()
        for freq in self.freq_set:
            self.setdata_freq(freq)
            pos_df_per_freq = pd.DataFrame()
            for half_life in self.half_life_set:
                pos_df_per_half_life = pd.DataFrame()
                for ticker in self.close_df.columns:
                    ttt_close = self.close_df.loc[:, ticker]
                    ttt_high = self.high_df.loc[:, ticker]
                    ttt_low = self.low_df.loc[:, ticker]
                    ttt_open = self.open_df.loc[:, ticker]
                    ttt_volume = self.volume_df.loc[:, ticker]
                    dict_df = dict()
                    dict_df['high'] = ttt_high.copy()
                    dict_df['low'] = ttt_low.copy()
                    dict_df['close'] = ttt_close.copy()
                    dict_df['open'] = ttt_open.copy()
                    dict_df['volume'] = ttt_volume.copy()
                    ttt_close_spot = self.close_df_spot.loc[:, ticker]
                    logreturn = np.log(ttt_close).diff()
                    logreturn.name = 'target'

                    # factors
                    # temp_expression = 'mytest(dict_df, half_life)'
                    thefactor = eval(temp_expression) ### calc_aroon(ttt_high, ttt_low, half_life)
                    series = np.power(logreturn, 2)
                    thecarry = np.log(ttt_close_spot / ttt_close)

                    factor_df = pd.concat([thecarry, thefactor], axis=1)
                    factor_df.columns = ['factor' + str(x) for x in range(0, factor_df.shape[1])]

                    prediction_shift = self.prediction_shift
                    df = pd.concat([factor_df.shift(prediction_shift), logreturn.rolling(prediction_shift).mean()],
                                   axis=1)
                    roll_reg = RollingOLS(df.target, add_constant(df.loc[:, factor_df.columns]), window=half_life)
                    model = roll_reg.fit(params_only=True)
                    coefficient_df = model.params

                    factor_df['const'] = 1
                    target_prediction = (coefficient_df.rolling(int(half_life / 10)).mean() * factor_df).sum(1)

                    # add smooth factor which is particular useful when the sample frequency is small like 90min.
                    anualized_factor = 365.0 * pd.to_timedelta('24hours') / pd.to_timedelta(freq)
                    smooth_factor = 2
                    pos_temp = np.sign(target_prediction.rolling(smooth_factor).mean()) / (
                            logreturn.rolling(half_life).std() * np.sqrt(anualized_factor))
                    pos_temp.name = ticker
                    pos_df_per_half_life = pos_df_per_half_life.add(pd.DataFrame(pos_temp), fill_value=0)
                pos_df_per_freq = pos_df_per_freq.add(pos_df_per_half_life, fill_value=0)
            pos_df_per_freq = pos_df_per_freq / len(self.half_life_set)
            pos_df_ = pd.concat([pos_df_, pos_df_per_freq], axis=1)

        signal_df_ = pos_df_.fillna(method='ffill')
        signal_df = pd.DataFrame()
        for i in range(0, signal_df_.shape[1]):
            signal_df = signal_df.add(signal_df_.iloc[:, [i]], fill_value=0)
        signal_df = signal_df.loc[:, signal_df.columns.sort_values()] / len(self.freq_set)
        signal_df = signal_df / signal_df.shape[1]

        close_df = self.raw_close_df.loc[signal_df.index.intersection(self.raw_close_df.index), :]
        self.assetreturn = np.log(close_df).diff()

        normalize_lookbackperiod = self.normalize_lookbackperiod
        normalize_minlookbackperiod = int(normalize_lookbackperiod / 2)
        anualized_factor = 365.0 * pd.to_timedelta('24hours') / pd.to_timedelta(self.freq_set[-1])
        momsignal = fundlevelnormalization(momsignal=signal_df, fonbday1=self.assetreturn, voltarget=self.voltarget,
                                           normalize_lookbackperiod=normalize_lookbackperiod,
                                           normalize_minlookbackperiod=normalize_minlookbackperiod,
                                           factor=anualized_factor, flexstd=True)
        momsignal_xs = fundlevelnormalization(momsignal=XSnormalization(signal_df), fonbday1=self.assetreturn,
                                              voltarget=self.voltarget,
                                              normalize_lookbackperiod=normalize_lookbackperiod,
                                              normalize_minlookbackperiod=normalize_minlookbackperiod,
                                              factor=anualized_factor, flexstd=True)
        momsignal = risk_manage_asset(risk_manage_portfolio(momsignal, self.portfolio_limit), self.asset_limit)
        momsignal_xs = risk_manage_asset(risk_manage_portfolio(momsignal_xs, self.portfolio_limit), self.asset_limit)
        momsignal = signalround(momsignal)
        momsignal_xs = signalround(momsignal_xs)
        self.allocation = {'TS': momsignal,
                           'XS': momsignal_xs}
