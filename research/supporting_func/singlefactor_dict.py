from research.supporting_func.bs import bs
from research.supporting_func.dataresampler import DataResampler
from research.supporting_func.supportingfunctions import print_stat, XSnormalization, \
    fundlevelnormalization, risk_manage_portfolio, risk_manage_asset, signalround
from research.data.histbarreader import HistBarReader
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time


# def toexec(open, high, low, close, volume, lookbackperiod):
#     return calc_rsi(close, lookbackperiod)


### signal --> signal
def series_rank(series, lookbackperiod):
    temp = series.rolling(lookbackperiod).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
    return (temp - 0.5) * 2


def series_hlnormalization(series, lookbackperiod):
    s_high = series.rolling(lookbackperiod).max()
    s_low = series.rolling(lookbackperiod).min()
    sind = (series - s_low) / (s_high - s_low)
    return (sind - 0.5) * 2


def series_meanstdnormalization(series, lookbackperiod):
    sma = series.rolling(lookbackperiod).mean()
    std = series.rolling(lookbackperiod).std()
    return (series - sma) / std


def series_diff(series, lookbackperiod):
    return series.diff(lookbackperiod)


### series --> Data
def Series_OHLC(series, lookbackperiod):
    dict2return = dict()
    dict2return['close'] = series.copy()
    dict2return['open'] = series.shift(lookbackperiod)
    dict2return['high'] = series.rolling(lookbackperiod).max().copy()
    dict2return['low'] = series.rolling(lookbackperiod).min().copy()
    dict2return['volume'] = series / series
    return dict2return


### Data -->Data
def Data_OHLC(dict_df, lookbackperiod):
    dict2return = dict()
    dict2return['close'] = dict_df['close'].copy()
    dict2return['open'] = dict_df['open'].shift(lookbackperiod).copy()
    dict2return['high'] = dict_df['high'].rolling(lookbackperiod).max()
    dict2return['low'] = dict_df['low'].rolling(lookbackperiod).min()
    dict2return['volume'] = dict_df['volume'].rolling(lookbackperiod).sum()
    return dict2return


### Series -->UpDown
def SeriesUD(series):
    delta = np.log(series).diff().fillna(0)
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    dict_UpDown = dict()
    dict_UpDown['up'] = up.copy()
    dict_UpDown['down'] = down.copy()
    return dict_UpDown


def SeriesMaxMin(series, lookbackperiod):
    up = series.rolling(lookbackperiod).apply(lambda x: x.argmax())
    down = series.rolling(lookbackperiod).apply(lambda x: x.argmin())
    dict_UpDown = dict()
    dict_UpDown['up'] = up.copy()
    dict_UpDown['down'] = down.copy()
    return dict_UpDown


### Data -->UpDown
def ReturnUD(dict_df):
    delta = np.log(dict_df['close']).diff().fillna(0)
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    dict_UpDown = dict()
    dict_UpDown['up'] = up.copy()
    dict_UpDown['down'] = down.copy()
    return dict_UpDown


def ReturnTPVolumeUD(dict_df, lookbackperiod):
    tp = (dict_df['close'] + dict_df['high'] + dict_df['low']) / 3
    delta = np.log(tp).diff()
    up = (delta.clip(lower=0) * dict_df['volume']).rolling(lookbackperiod).sum()
    down = -1 * (delta.clip(upper=0) * dict_df['volume']).rolling(lookbackperiod).sum()
    dict_UpDown = dict()
    dict_UpDown['up'] = up.copy()
    dict_UpDown['down'] = down.copy()
    return dict_UpDown


def DataMaxMin(dict_df, lookbackperiod):
    up = dict_df['high'].rolling(lookbackperiod).apply(lambda x: x.argmax())
    down = dict_df['low'].rolling(lookbackperiod).apply(lambda x: x.argmin())
    dict_UpDown = dict()
    dict_UpDown['up'] = up.copy()
    dict_UpDown['down'] = down.copy()
    return dict_UpDown


### UpDown --> signal:
def SimpleRatio(dict_UpDown):
    up = dict_UpDown['up']
    down = dict_UpDown['down']
    return (up - down) / (up + down)


### Dict --> Dict: (updown, Data)
def dict_mean(dict_, lookbackperiod):
    dict2return = dict()
    for k, v in dict_.items():
        dict2return[k] = dict_[k].rolling(lookbackperiod).mean()
    return dict2return


def dict_diff(dict_, lookbackperiod):
    dict2return = dict()
    for k, v in dict_.items():
        dict2return[k] = dict_[k].diff(lookbackperiod)
    return dict2return


def dict_log(dict_):
    dict2return = dict()
    for k, v in dict_.items():
        dict2return[k] = np.log(dict_[k])
    return dict2return


### Data --> signal
def vbo(dict_df, lookbackperiod):
    return dict_df['close'].diff(lookbackperiod) * dict_df['volume']


def c1trend(dict_df, lookbackperiod):
    return np.sign(dict_df['close'].diff(lookbackperiod))

def clv(dict_df):
    return (dict_df['close'] - dict_df['open']) / (dict_df['high'] - dict_df['low'])


def calc_adx(dict_df, lookbackperiod):
    negative_dm = (dict_df['low'].shift(lookbackperiod) - dict_df['low']).clip(lower=0)
    positive_dm = (dict_df['high'] - dict_df['high'].shift(lookbackperiod)).clip(lower=0)
    return (positive_dm - negative_dm) / (dict_df['high'] - dict_df['low']).rolling(lookbackperiod).mean()


def calc_emv(dict_df, lookbackperiod):
    dm = (dict_df['high'] + dict_df['low']) - (
            dict_df['high'].shift(lookbackperiod) + dict_df['low'].shift(lookbackperiod))
    br = dict_df['volume'] / (dict_df['high'] - dict_df['low'])
    emv = dm / br.rolling(lookbackperiod).mean()
    return series_rank(emv, lookbackperiod)


### example:
### clv(Series_OHLC(SimpleRatio(SeriesMaxMin(calc_bop(dict_df, lookbackperiod), lookbackperiod)), lookbackperiod))
### clv(Series_OHLC(series_diff(clv(Data_OHLC(dict_df, lookbackperiod)), lookbackperiod), lookbackperiod))
def mytest(dict_df, lookbackperiod):
    return series_hlnormalization(calc_cii(dict_df, lookbackperiod), lookbackperiod)


def calc_bop(dict_df, lookbackperiod):
    return clv(Data_OHLC(dict_df, lookbackperiod))


def calc_rsi(dict_df, lookbackperiod):
    return SimpleRatio(dict_mean(ReturnUD(dict_df), lookbackperiod))


def calc_srsi(dict_df, lookbackperiod):
    return series_hlnormalization(SimpleRatio(dict_mean(ReturnUD(dict_df), lookbackperiod)), lookbackperiod)


def calc_aroon(dict_df, lookbackperiod):
    return SimpleRatio(DataMaxMin(dict_df, lookbackperiod))


def calc_mfi(dict_df, lookbackperiod):
    return SimpleRatio(ReturnTPVolumeUD(dict_df, lookbackperiod))


def calc_cii(dict_df, lookbackperiod):
    tp = (dict_df['close'] + dict_df['high'] + dict_df['low']) / 3
    return series_rank(series_meanstdnormalization(tp, lookbackperiod), lookbackperiod)


def calc_fi(dict_df, lookbackperiod):
    return series_rank(vbo(dict_df, lookbackperiod), lookbackperiod)


class singlefactor_dict(bs):
    def __init__(self, logreturn, high, low, close, open, volume, anualized_factor, name, smooth_factor,
                 temp_expression, normalize_lookbackperiod=120, std_cap=0.1, voltarget=0.15, portfolio_limit=1,
                 asset_limit=0.3):
        super(singlefactor_dict, self).__init__(name=name, anualized_factor=anualized_factor)
        self.logreturn = logreturn
        self.high = high
        self.low = low
        self.close = close
        self.open = open
        self.volume = volume
        self.normalize_lookbackperiod = normalize_lookbackperiod
        self.normalize_minlookbackperiod = int(normalize_lookbackperiod / 2)
        self.std_cap = std_cap
        self.voltarget = voltarget
        self.smooth_factor = smooth_factor
        self.portfolio_limit = portfolio_limit
        self.asset_limit = asset_limit
        self.lookbackperiod = None
        self.minlookbackperiod = None
        self.temp_expression = temp_expression

    def toexec(self, dict_df, lookbackperiod, temp_expression):  ## open, high, low, close, volume
        return eval(temp_expression)

    def get_logreturn(self):
        return self.logreturn

    def get_high(self):
        return self.high

    def get_low(self):
        return self.low

    def get_close(self):
        return self.close

    def get_open(self):
        return self.open

    def get_volume(self):
        return self.volume

    def get_lookbackperiod(self):
        return self.lookbackperiod

    def set_lookbackperiod(self, lookbackperiod):
        self.lookbackperiod = lookbackperiod
        self.minlookbackperiod = int(lookbackperiod[0] / 2)

    def predict(self):
        fonbday1 = self.get_logreturn()
        high = self.get_high()
        low = self.get_low()
        close = self.get_close()
        open = self.get_open()
        volume = self.get_volume()
        dict_df = dict()
        dict_df['high'] = high.copy()
        dict_df['low'] = low.copy()
        dict_df['close'] = close.copy()
        dict_df['open'] = open.copy()
        dict_df['volume'] = volume.copy()
        roll120std = fonbday1.rolling(self.normalize_lookbackperiod,
                                      min_periods=self.normalize_minlookbackperiod).std() * np.sqrt(
            self.get_anualized_factor())
        TSnormalization_instruments = self.std_cap / \
                                      ((roll120std < self.std_cap) * self.std_cap + (
                                              roll120std >= self.std_cap) * roll120std)
        tempsignal = (fonbday1 * 0).fillna(method='ffill').fillna(0)
        tempsignal_xs = tempsignal.copy()
        tempcount = tempsignal.copy()
        for i in self.get_lookbackperiod():
            ## close=close, high=high, low=low, open=open, volume=volume
            haha = self.toexec(dict_df=dict_df, lookbackperiod=i,
                               temp_expression=self.temp_expression)
            # haha = calc_bop(high, low, close, i)
            # haha = np.sign(fonbday2.rolling(i, min_periods=self.minlookbackperiod).mean())
            tempsignal = tempsignal + haha.fillna(0)
            tempsignal_xs = tempsignal_xs + XSnormalization(haha)
            tempcount = tempcount + (1.0 - np.isnan(haha))

        momsingal_xs = XSnormalization(tempsignal_xs / tempcount)
        momsingal_xs = fundlevelnormalization(momsignal=momsingal_xs, fonbday1=fonbday1, voltarget=self.voltarget,
                                              normalize_lookbackperiod=self.normalize_lookbackperiod,
                                              normalize_minlookbackperiod=self.normalize_minlookbackperiod,
                                              factor=self.get_anualized_factor(), flexstd=True)

        momsignal = TSnormalization_instruments * tempsignal / tempcount
        momsignal = fundlevelnormalization(momsignal=momsignal, fonbday1=fonbday1, voltarget=self.voltarget,
                                           normalize_lookbackperiod=self.normalize_lookbackperiod,
                                           normalize_minlookbackperiod=self.normalize_minlookbackperiod,
                                           factor=self.get_anualized_factor(), flexstd=True)

        tempts = risk_manage_asset(
            risk_manage_portfolio(momsignal.rolling(self.smooth_factor).mean(), self.portfolio_limit), self.asset_limit)
        tempxs = risk_manage_asset(
            risk_manage_portfolio(momsingal_xs.rolling(self.smooth_factor).mean(), self.portfolio_limit),
            self.asset_limit)
        self.allocation = {'TS': signalround(tempts),
                           'XS': signalround(tempxs)}


if __name__ == '__main__':
    numberofdatalines = None
    base_currency = 'USDT'
    exchange = 'BINANCE_USDT'
    debug = True
    pair_name_set = ['BTC', 'ETH', 'MATIC', 'SOL', 'MANA', '1000SHIB']
    # pair_name_set = ['DOGE', '1000SHIB',
    #                  'HNT', 'GTC', 'CTK', 'NKN', 'LPT', 'ANKR', 'DENT',
    #                  'SAND', 'GALA', 'MANA', 'AXS', 'ALICE',
    #                  'CHZ', 'THETA', 'ENJ', 'AUDIO', 'BAKE', 'OGN',
    #                  'TRB', 'LINK', 'RLC', 'GRT', 'BAND',
    #                  'C98', 'MTL', 'SFP',
    #                  'FIL', 'AR', 'OCEAN', 'STORJ', 'ONT', 'BLZ',
    #                  'ZEC', 'XMR', 'LIT', 'MASK', 'ATA', 'ZEN', 'ARPA', 'DUSK',
    #                  'CRV', 'AAVE', 'COMP', 'KNC', 'ALPHA', 'YFI', 'ROSE', 'REN', 'UNFI', 'CVX', 'LINA', 'MKR', 'BAT',
    #                  'REEF', 'RSR', 'BEL', 'API3', 'SPELL', 'FLM']

    start = time.time()
    raw_data = dict()
    for symbol in pair_name_set:
        histbarreader = HistBarReader(pair_name=symbol, number_of_date=None, base_currency=base_currency,
                                      exchange=exchange, debug=debug)
        histbarreader.readdata()
        tempdata = histbarreader.getdata()
        raw_data[symbol] = tempdata
    endofreading = time.time()

    resample_period = '60min'
    port_bars = dict()
    for symbol in pair_name_set:
        tempdata = raw_data[symbol]
        dataresample = DataResampler(resample_period)
        resampled_tempdata = dataresample.dataresampler(tempdata)
        port_bars[symbol] = resampled_tempdata
    assetclose = pd.DataFrame({s: bars['close'] for s, bars in port_bars.items()})
    assethigh = pd.DataFrame({s: bars['high'] for s, bars in port_bars.items()})
    assetlow = pd.DataFrame({s: bars['low'] for s, bars in port_bars.items()})
    assetopen = pd.DataFrame({s: bars['open'] for s, bars in port_bars.items()})
    assetvolume = pd.DataFrame({s: bars['volume'] for s, bars in port_bars.items()})
    assetreturn = np.log(assetclose).diff()

    print('start model')

    anualized_factor = 365.0 * pd.to_timedelta('24hours') / pd.to_timedelta(resample_period)

    # because crypto has a relative higher short term vol, we use shorter lookbackperiod to adjust
    normalize_lookbackperiod = int(pd.to_timedelta('24hours') / pd.to_timedelta(resample_period)) * 1
    smooth_factor = int(pd.to_timedelta('1hours') / pd.to_timedelta(resample_period)) * 12
    # templookbackperiod=range(30, 151, 10)
    templookbackperiod = range(10 * int(pd.to_timedelta('48h') / pd.to_timedelta(resample_period)),
                               51 * int(pd.to_timedelta('48h') / pd.to_timedelta(resample_period)),
                               5 * int(pd.to_timedelta('48h') / pd.to_timedelta(resample_period)))

    temp_expression = 'series_rank(series_hlnormalization(c1trend(dict_df, lookbackperiod), lookbackperiod), lookbackperiod)'
    # stochastic: mom
    # srsi: mom on mom
    # fi: rank on mom
    # aroon: mom in terms of index
    trendsignal = singlefactor_dict(name='c4', logreturn=assetreturn, high=assethigh, low=assetlow,
                                    close=assetclose, open=assetopen, volume=assetvolume,
                                    temp_expression=temp_expression, normalize_lookbackperiod=normalize_lookbackperiod,
                                    anualized_factor=anualized_factor, smooth_factor=smooth_factor)
    trendsignal.set_lookbackperiod(templookbackperiod)
    trendsignal.predict()
    endofpredict = time.time()

    signal_shift = 2
    resample_shift = 1
    todaystr = pd.Timestamp("today").strftime("%Y-%m-%d")
    tempsignal = (trendsignal.get_allocation()['TS']).shift(signal_shift).loc['2015-1-1':todaystr]
    assetreturn = assetreturn.loc['2015-1-1':todaystr]
    tempsignal = tempsignal.resample(resample_period, label='right', closed='right').last().shift(resample_shift)
    assetreturn = assetreturn.resample(resample_period, label='right', closed='right').sum()
    (tempsignal * assetreturn).sum(axis=1).cumsum().plot()
    plt.show(block=True)

    print('correlation_matrix')
    print((tempsignal * assetreturn).corr())
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=True,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=False,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    tempsignal.to_csv('c4_TS_pos.csv')

    tempsignal = (trendsignal.get_allocation()['XS']).shift(signal_shift).loc['2015-1-1':todaystr]
    assetreturn = assetreturn.loc['2015-1-1':todaystr]
    tempsignal = tempsignal.resample(resample_period, label='right', closed='right').last().shift(resample_shift)
    assetreturn = assetreturn.resample(resample_period, label='right', closed='right').sum()
    (tempsignal * assetreturn).sum(axis=1).cumsum().plot()
    plt.show()

    print('correlation_matrix')
    print((tempsignal * assetreturn).corr())
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=True,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    print_stat(tempsignal=tempsignal, assetreturn=assetreturn,
               anualized_factor=anualized_factor, numberofdays2trade=365, ind=False,
               items=['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover'])
    tempsignal.to_csv('c4_XS_pos.csv')

    print('reading data takes: ' + str(endofreading - start))
    print('prediction takes: ' + str(endofpredict - start))
    print('end')
