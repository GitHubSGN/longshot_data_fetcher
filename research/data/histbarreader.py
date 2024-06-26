from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import time
from pathlib import Path


class BarReader():
    def __init__(self, base_currency, pair_name=None, exchange='BINANCE_USDT', debug=False):
        self.pair_name = None
        self.base_currency = base_currency
        self.data = None
        self.debug = debug
        self.exchange = exchange
        self.set_pair_name(pair_name=pair_name)

    def getdata(self):
        return self.data

    def setsqlreq(self):
        pass

    def set_pair_name(self, pair_name):
        if pair_name is not None:
            self.pair_name = pair_name
            self.setsqlreq()

    def get_pair_name(self):
        return self.pair_name

    def _readdata(self):
        return None

    def readdata(self):
        alldata = None
        while alldata is None:
            try:
                alldata = self._readdata()
            except:
                print(f'there is something wrong when reading {self.pair_name}. will try again after 10 sec')
                time.sleep(10)
                pass
        self.data = alldata
        self.organizedata()

    def readsetdata(self, pair_name_set, returntype='return'):
        toreturn = pd.DataFrame()
        for pair_name in pair_name_set:
            self.set_pair_name(pair_name=pair_name)
            self.readdata()
            PVdata1 = self.getdata().copy()
            PVdata1.index = pd.to_datetime(PVdata1.index)
            price_temp = PVdata1.loc[:, ['close']]
            price_temp.columns = [self.get_pair_name()]
            toreturn = pd.concat([toreturn, price_temp], axis=1).copy()
        if returntype == 'return':
            toreturn = np.log(toreturn).diff()
        return toreturn

    def readohlcv(self, pair_name_set, datacol='close'):
        toreturn = pd.DataFrame()
        for pair_name in pair_name_set:
            self.set_pair_name(pair_name=pair_name)
            self.readdata()
            PVdata1 = self.getdata().copy()
            PVdata1.index = pd.to_datetime(PVdata1.index)
            price_temp = PVdata1.loc[:, [datacol]]
            price_temp.columns = [self.get_pair_name()]
            toreturn = pd.concat([toreturn, price_temp], axis=1).copy()
        return toreturn

    def organizedata(self):
        mydata = self.getdata()
        # mydata = self.getdata().loc[:, ['close_price', 'high_price', 'low_price', 'open_price', 'volume']]
        # mydata.columns = ['close', 'high', 'low', 'open', 'volume']
        # # mydata.index = pd.DatetimeIndex(
        # #     [pd.Timestamp(mytime(values=x, denominator=1e6, microseconds=False)) for x in self.getdata().end_time])
        # mydata.index = pd.DatetimeIndex([datetime.utcfromtimestamp(x / 1e6) for x in self.getdata().end_time])
        # mydata.index.name = None
        # if self.debug:
        #     mydata.to_csv('raw.csv')
        #
        # mydata.sort_index(inplace=True)
        # mydata.index = mydata.index.ceil('T')
        # mydata.index = [x.strftime("%Y-%m-%d %H:%M:%S") for x in mydata.index]
        # mydata = mydata[~mydata.index.duplicated(keep='first')]
        # if self.debug:
        #     mydata.to_csv('organized.csv')
        self.data = mydata.astype('float64')


class HistBarReader(BarReader):
    def __init__(self, base_currency, pair_name=None, number_of_date=None, exchange='BINANCE_USDT', debug=False):
        if not number_of_date:
            number_of_date = 1500
        self.number_of_date = number_of_date
        super().__init__(base_currency, pair_name,
                         exchange, debug)

    def setsqlreq(self):
        pass

    def _readdata(self):
        # ch_config = ChConfig()
        # temp_ch = ChReader(ch_config.HOST, ch_config.USER, ch_config.PASSWORD, "coin_hist")
        # if self.debug:
        #     print(self.sql)
        #     start = time.time()
        # alldata = temp_ch.query_df(cmd=self.sql)
        ttt = pd.read_csv(Path(__file__).parent / 'saveddata' / self.exchange.lower() / (self.pair_name + self.base_currency + '.csv'))
        ttt.index = ttt.iloc[:, 0]
        ttt.index.name = None
        ttt.drop(ttt.columns[[0]], axis=1, inplace=True)
        alldata = ttt.iloc[-int(pd.to_timedelta('24h') / pd.to_timedelta('1min') * self.number_of_date):,]
        # refilldatareader = RealtimeBarReader(pair_name=self.pair_name, numberofdatalines=None, reader_prefix='listbar_bk',
        #                                      base_currency=self.base_currency, exchange=self.exchange, debug=self.debug)
        # refilldata = refilldatareader._readdata()
        # if refilldata.empty is not True:
        #     alldata = pd.concat([alldata, refilldata], axis=0, ignore_index=True)
        # if self.debug:
        #     endofreading = time.time()
        #     print(f'reading data takes: {endofreading - start}')
        return alldata


class RealtimeBarReader(BarReader):
    def __init__(self, base_currency, pair_name=None, numberofdatalines=None, numberofdatalines_=60,
                 exchange='BINANCE_USDT', reader_prefix="listbar", debug=False):
        self.reader_key = None
        self.numberofdatalines = numberofdatalines
        self.numberofdatalines_ = numberofdatalines_
        self.iffirsttime = None
        self.reader_prefix = reader_prefix
        super().__init__(base_currency, pair_name, exchange, debug)

    def setsqlreq(self):
        pass

    def _readdata(self):
        # redis_config = RedisConfig()
        # redis_reader = RedisListReader(redis_config.REDIS_HOST, redis_config.REDIS_PWD, redis_config.REDIS_DB)
        # if self.debug:
        #     print(self.reader_key)
        #     start = time.time()
        # if self.iffirsttime:
        #     templines2read = int(self.numberofdatalines)
        # else:
        #     templines2read = int(self.numberofdatalines_)
        # if self.debug:
        #     print(f'we read data of {templines2read} lines.')
        # alldata = redis_reader.read_data(self.reader_key, templines2read)
        # alldata = pd.json_normalize([asdict(obj) for obj in alldata])
        # if alldata.empty is not True:
        #     alldata = alldata.loc[:,
        #               ['start_time', 'end_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        # else:
        #     print(f'{self.reader_key} is empty')
        # if self.debug:
        #     endofreading = time.time()
        #     print(f'reading data takes: {endofreading - start}')
        # self.iffirsttime = False
        return pd.DataFrame()


if __name__ == '__main__':
    exchange = 'BINANCE_SPOT'  # BINANCE_SPOT
    base_currency = 'USDT'
    pair_name = 'ETH'
    number_of_date = 30  # None
    numberofdatalines = None
    ifhistorical = False
    debug = True

    histbarreader = HistBarReader(pair_name=pair_name, number_of_date=number_of_date, base_currency=base_currency,
                                  exchange=exchange, debug=debug)
    histbarreader.readdata()
    print(histbarreader.getdata())

    histbarreader = RealtimeBarReader(pair_name=pair_name, numberofdatalines=numberofdatalines,
                                      base_currency=base_currency, exchange=exchange, debug=debug)
    histbarreader.readdata()
    print(histbarreader.getdata())

    histbarreader.readdata()
    print(histbarreader.getdata())
