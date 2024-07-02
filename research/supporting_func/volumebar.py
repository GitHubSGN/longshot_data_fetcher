import pandas as pd


class VolumeBar():
    def __init__(self, lookbackperiod, resampleperiod, data_freq, debug=False):
        self.lookbackperiod = lookbackperiod
        self.resampleperiod = resampleperiod
        self.data_freq = data_freq
        self.debug = debug

        self.allocated_data = None
        self.unallocated_data = None
        self.usd_volume_data_set = None   ### contain the recent 7 days volume data
        self.last_index = None

        self.volume = None
        self.open = None
        self.high = None
        self.low = None
        self.close = None

    def organize_incoming_data(self, df):
        self.unallocated_data = pd.concat([self.unallocated_data, df], axis=0)
        self.unallocated_data.sort_index(inplace=True)
        self.unallocated_data = self.unallocated_data[~self.unallocated_data.index.duplicated(keep='first')]

        combined_usd_volume_data_set = pd.concat([self.usd_volume_data_set, self.unallocated_data.volume * self.unallocated_data.close], axis=0)
        combined_usd_volume_data_set.name = 'usd_volume'
        usd_volume_threshold = combined_usd_volume_data_set.rolling(
            int(pd.Timedelta(self.lookbackperiod) / self.data_freq)).mean() * (
                                       pd.Timedelta(self.resampleperiod) / self.data_freq)
        usd_volume_threshold.name = 'usd_volume_threshold'
        df_vol = pd.concat([self.unallocated_data, combined_usd_volume_data_set, usd_volume_threshold], axis=1).reindex(self.unallocated_data.index)

        self.new_row()
        for index, row in df_vol.iterrows():
            self.volume = self.volume + row['usd_volume']
            self.close = row.close
            if self.open is None:
                self.open = row.open
                self.high = row.high
                self.low = row.low
            else:
                if self.high < row.high:
                    self.high = row.high
                if self.low > row.low:
                    self.low = row.low
            if self.volume > row['usd_volume_threshold']:
                temp_df = pd.DataFrame([[self.close, self.high, self.low, self.open, self.volume]],
                                       columns=['close', 'high', 'low', 'open', 'volume'], index=[index])
                self.allocated_data = pd.concat([self.allocated_data, temp_df], axis=0)
                self.new_row()
                self.last_index = index
                if self.debug:
                    # self.unallocated_data = self.unallocated_data.loc[self.unallocated_data.index > self.last_index,]
                    print(f'the last index is {self.last_index}')
                    # print(self.allocated_data)
                    # print(self.unallocated_data)
        if self.last_index is not None:
            if (self.unallocated_data.index > self.last_index).sum() > 0:
                self.unallocated_data = self.unallocated_data.loc[self.unallocated_data.index > self.last_index,]  ### test last index is none or no index is bigger than the last index.
            else:
                self.unallocated_data = None
        self.usd_volume_data_set = combined_usd_volume_data_set.tail(int(pd.Timedelta(self.lookbackperiod) / self.data_freq))
        if self.debug:
            print(f'the last index is {self.last_index}')
            print(self.allocated_data)
            print(self.unallocated_data)

    def new_row(self):
        self.volume = 0.0
        self.open = None
        self.high = None
        self.low = None
        self.close = None

    def get_allocated_data(self):
        return self.allocated_data

    def get_unallocated_data(self):
        return self.unallocated_data

    def get_last_index(self):
        return self.last_index


if __name__ == '__main__':
    from research.data.histbarreader import HistBarReader
    pair_name = 'BTC'
    numberofdatalines = 1440
    base_currency = 'USDT'
    exchange = 'BINANCE_USDT'
    debug = True
    histbarreader = HistBarReader(pair_name=pair_name, number_of_date=numberofdatalines, base_currency=base_currency,
                                  exchange=exchange, debug=debug)
    histbarreader.readdata()
    tempdata = histbarreader.getdata()
    print(tempdata)

    lookback_period_volumebar = '7d'
    resample_period = '60min'
    data_freq = '1min'
    debug = True
    port_bars = dict()
    volumebar_datareader = VolumeBar(lookbackperiod=lookback_period_volumebar, resampleperiod=resample_period, data_freq=data_freq, debug=debug)

    volumebar_datareader.organize_incoming_data(tempdata)
    allocated_data = volumebar_datareader.get_allocated_data()
    unallocated_data = volumebar_datareader.get_unallocated_data()
    print(f'the last index is {volumebar_datareader.get_last_index()}')
