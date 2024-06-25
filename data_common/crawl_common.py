import pandas as pd
import time
import datetime as dt
import ccxt

from tools.date_util import datetime_to_timestamp_tz0


def convert_to_dataframe(all_ohlcvs, symbol,end_time,exchange):
    df = pd.DataFrame(all_ohlcvs, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = df[df['timestamp'] < end_time]
    df['timestamp'] = df['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df['symbol'] = symbol
    df['exchange'] = exchange
    df = df.rename(columns={'timestamp': 'open_time'})
    return df

def timeframe_to_milliseconds(timeframe):
    seconds_per_unit = {
        's': 1,
        'm': 60,
        'h': 60 * 60,
        'd': 24 * 60 * 60,
        'w': 7 * 24 * 60 * 60,
        'M': 30 * 24 * 60 * 60
    }
    unit = timeframe[-1]
    if unit not in seconds_per_unit:
        raise ValueError(f'Invalid timeframe: {timeframe}')
    try:
        n = int(timeframe[:-1])
    except ValueError:
        raise ValueError(f'Invalid timeframe: {timeframe}')
    return n * seconds_per_unit[unit] * 1000

def get_ohlcv_df(symbol,start_time_str,end_time_str,exchange,timeframe,limit):
    # 调整时间格式
    exchange = eval('ccxt.' + exchange + '()')
    try:
        start_time = dt.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
    except:
        start_time = dt.datetime.strptime(start_time_str, '%Y-%m-%d')
    # start_time = int(start_time.timestamp() * 1000)
    start_time = datetime_to_timestamp_tz0(start_time)
    try:
        end_time = dt.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
    except:
        end_time = dt.datetime.strptime(end_time_str, '%Y-%m-%d')
    # end_time = int(end_time.timestamp() * 1000)
    end_time = datetime_to_timestamp_tz0(end_time)

    all_ohlcvs = []
    restart_time = start_time

    # 取到指定时间
    while restart_time < end_time:
        ohlcvs = exchange.fetch_ohlcv(symbol, timeframe= timeframe, since=restart_time, limit=limit)
        print(dt.datetime.fromtimestamp(restart_time / 1000).strftime('%Y-%m-%d %H:%M:%S'))
        if len(ohlcvs)>0:
            if restart_time < ohlcvs[0][0] - 60000:
                restart_time = ohlcvs[0][0]
            else:
                all_ohlcvs += ohlcvs
                time.sleep(exchange.rateLimit / 1000)
                restart_time = restart_time + (timeframe_to_milliseconds(timeframe) * limit)
        else:
            print('else')
            restart_time = restart_time + (timeframe_to_milliseconds(timeframe) * limit)

    df = convert_to_dataframe(all_ohlcvs,symbol,end_time,exchange)
    # print('{}'.format(symbol + ' ' + str(exchange)
    #                         ) + '----------------------start-------------------------\n')
    return df

def get_funding_df(symbol,start_time_str,end_time_str,exchange):
    # 调整时间格式
    exchange = eval('ccxt.' + exchange + '()')
    try:
        start_time = dt.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
    except:
        start_time = dt.datetime.strptime(start_time_str, '%Y-%m-%d')
    # start_time = int(start_time.timestamp() * 1000)
    start_time = datetime_to_timestamp_tz0(start_time)
    try:
        end_time = dt.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
    except:
        end_time = dt.datetime.strptime(end_time_str, '%Y-%m-%d')
    # end_time = int(end_time.timestamp() * 1000)
    end_time = datetime_to_timestamp_tz0(end_time)

    df = exchange.fetchFundingRateHistory (symbol = symbol, since = start_time, params = {"endTime":end_time} )
    df = pd.DataFrame(df, columns=["symbol", "fundingRate", "timestamp", "datetime"])

    return df

if __name__ == "__main__":
    start_time_str = '2023-01-01'
    end_time_str = '2024-05-13'
    exchange = 'okx'
    # symbol = 'ORDI/USDT:USDT'
    symbol = "ORDI/USDT"
    timeframe = '1h'
    limit = 1000
    # df = get_ohlcv_df(symbol, start_time_str, end_time_str, exchange, timeframe, limit)
    fdf = get_funding_df("AVAX/USDT:USDT", start_time_str, end_time_str, exchange)

    print("Done.")