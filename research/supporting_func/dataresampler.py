import pandas as pd


class DataResampler():
    def __init__(self, thefreq):
        self.thefreq = thefreq

    def getfreq(self):
        return self.thefreq

    def setfreq(self, thefreq):
        self.thefreq = thefreq

    def dataresampler(self, mydata_):
        mydata = mydata_.copy()
        resample_period = self.getfreq()
        mydata.index = pd.DatetimeIndex(mydata.index.values)
        mydata_daily = mydata.resample(resample_period, offset=resample_period, label='right', closed='right').agg(
            {
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'open': 'first',
                'volume': 'sum'
            }
        )
        if mydata_daily.index[-1] > mydata.index[-1]:
            mydata_daily.drop(mydata_daily.tail(1).index, inplace=True)
        return mydata_daily


if __name__ == '__main__':
    from data.histbarreader import HistBarReader

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

    dataresample = DataResampler('10min')
    resampled_tempdata = dataresample.dataresampler(tempdata)
    print(resampled_tempdata)
