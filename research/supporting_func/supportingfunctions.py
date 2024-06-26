import datetime
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from scipy.stats import norm


def mytime(values, denominator=1e6, microseconds=False):
    if microseconds:
        return datetime.datetime.utcfromtimestamp(values / denominator).strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        return datetime.datetime.utcfromtimestamp(values / denominator).strftime("%Y-%m-%d %H:%M:%S")
    # pd.to_datetime(mytime(1631755740000000)).tz_localize(tz='Asia/Shanghai').timestamp()
    # pd.to_datetime(mytime(1631755740000000)).timestamp()


def myUTCtime(ifstr):
    currenttime = datetime.datetime.utcnow()
    if ifstr:
        return currenttime.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return currenttime


def mystr2time(str, format="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(str, format)


def mylocaltime(values, denominator=1e6):
    return datetime.datetime.fromtimestamp(values / denominator).strftime("%Y-%m-%d %H:%M:%S")


def myreverttime(values, denominator=1e6):
    return int(pd.to_datetime(values).timestamp() * denominator)


def XSnormalization(tempsignal):
    temp = pd.concat([tempsignal.count(axis=1)] * tempsignal.shape[1], axis=1)
    temp.columns = tempsignal.columns
    toreturn = tempsignal.rank(axis=1, ascending=True, method='average') - (temp + 1) / 2
    toreturn_normalize = pd.concat([abs(toreturn.fillna(0)).sum(axis=1) * (abs(toreturn.fillna(0)).sum(axis=1) - 1)] *
                                   toreturn.shape[1], axis=1)
    toreturn_normalize.columns = toreturn.columns
    return (toreturn / toreturn_normalize).replace([np.inf, -np.inf], np.nan).fillna(0)


def fundlevelnormalization(momsignal, fonbday1, normalize_lookbackperiod=120,
                           normalize_minlookbackperiod=50, voltarget=0.15, thelag=2, factor=252, minstd=0.02,
                           flexstd=False):
    temp = (
            (momsignal.shift(thelag) * fonbday1).fillna(0).sum(axis=1).rolling(
                window=normalize_lookbackperiod, min_periods=normalize_minlookbackperiod).std()
            * np.sqrt(factor)
    )
    if flexstd:
        minstd = temp.rolling(normalize_lookbackperiod).quantile(0.05)
        minstd[minstd < 0.000002] = 0.02
        temp = temp * (temp >= minstd) + minstd * (temp < minstd)
    else:
        temp[temp < minstd] = minstd
    fundnormalization = voltarget / temp
    fundnormalization.replace([np.inf, -np.inf], np.nan, inplace=True)
    fundnormalization = pd.concat([fundnormalization] * momsignal.shape[1], axis=1).copy()
    fundnormalization.columns = momsignal.columns
    momsignal = (momsignal * fundnormalization).copy()
    return momsignal


def matrixnormalization(tempsignal, lookbackperiod):
    return (tempsignal - tempsignal.rolling(
        lookbackperiod, min_periods=int(lookbackperiod / 2.0)).mean()) / tempsignal.rolling(
        lookbackperiod, min_periods=int(lookbackperiod / 2.0)
    ).std()


def sharpe_ratio(tempsignal, assetreturn, factor=252, ind=True):
    pnl = tempsignal * assetreturn
    if ind:
        return pnl.mean() / pnl.std() * np.sqrt(factor)
    else:
        return pnl.sum(axis=1).mean() / pnl.sum(axis=1).std() * np.sqrt(factor)


def annualized_std(tempsignal, assetreturn, factor=252, ind=True):
    pnl = tempsignal * assetreturn
    if ind:
        return pnl.std() * np.sqrt(factor)
    else:
        return pnl.sum(axis=1).std() * np.sqrt(factor)


def sharpe_ratio_daily(tempsignal, assetreturn, factor=252, ind=True):
    pnl = (tempsignal * assetreturn).resample('1D').sum()
    if ind:
        return pnl.mean() / pnl.std() * np.sqrt(factor)
    else:
        return pnl.sum(axis=1).mean() / pnl.sum(axis=1).std() * np.sqrt(factor)


def annualized_std_daily(tempsignal, assetreturn, factor=252, ind=True):
    pnl = (tempsignal * assetreturn).resample('1D').sum()
    if ind:
        return pnl.std() * np.sqrt(factor)
    else:
        return pnl.sum(axis=1).std() * np.sqrt(factor)


def mymul(a, b):
    newdf = pd.concat([a, b], axis=1).fillna(method='ffill')
    return (newdf.iloc[:, 0] * newdf.iloc[:, 1]).to_frame()


def myadd(a, b):
    newdf = pd.concat([a, b], axis=1).fillna(method='ffill')
    return (newdf.iloc[:, 0] + newdf.iloc[:, 1]).to_frame()


def mymoment(normalizedclosematrix, n, power):
    smamatrix = normalizedclosematrix.rolling(n).mean()
    temp = normalizedclosematrix - smamatrix
    theup = temp.pow(power).rolling(n).mean()
    thedown = (temp.pow(2).rolling(n).mean()).pow(power / 2)
    toreturn = theup / thedown
    return toreturn


def myTII(mydata, n=14, m=9):
    smamatrix = mydata.rolling(n).mean()
    Dev = mydata - smamatrix
    posDev = Dev * (Dev > 0)
    negDev = -Dev * (Dev < 0)
    posDevmatrix = posDev.rolling(m).mean()
    negDevmatrix = negDev.rolling(m).mean()
    TII = 100 * posDevmatrix / (posDevmatrix + negDevmatrix)
    return TII


def risk_mange_truncation(signalmatrix, portfolio_limit=0.01):
    toreturn = np.sign(signalmatrix) * (np.abs(signalmatrix) - portfolio_limit).clip(lower=0)
    return toreturn


def risk_manage_portfolio(signalmatrix, portfolio_limit):
    temp_mul = portfolio_limit / np.abs(signalmatrix).sum(axis=1)
    temp_mul[temp_mul > 1.0] = 1.0
    return signalmatrix.mul(temp_mul, axis=0).copy()


def risk_manage_asset(signalmatrix, portfolio_limit, symbollist=None):
    if symbollist is None:
        temp_mul = portfolio_limit / np.abs(signalmatrix).max(axis=1)
        temp_mul[temp_mul > 1.0] = 1.0
    else:
        temp_mul = portfolio_limit / np.abs(signalmatrix.loc[:, signalmatrix.columns.isin(symbollist)]).max(axis=1)
        temp_mul[temp_mul > 1.0] = 1.0
    return signalmatrix.mul(temp_mul, axis=0).copy()


def risk_manage_cashdelta(signalmatrix, portfolio_limit=0.4):
    temp_mul = portfolio_limit / (np.abs(signalmatrix.sum(axis=1)) + 0.01)
    temp_mul[temp_mul > 1.0] = 1.0
    return signalmatrix.mul(temp_mul, axis=0).copy()


def profit_pertrade(tempsignal, assetreturn, ind=True):
    if ind:
        return (assetreturn * tempsignal).sum() / np.abs(tempsignal.diff()).sum()
    else:
        return (assetreturn * tempsignal).sum(axis=1).sum() / np.abs(tempsignal.diff()).sum(axis=1).sum()


def turnover(tempsignal, ind=True):
    if ind:
        return 1.0 / np.abs(tempsignal.diff()).mean()
    else:
        return 1.0 / np.abs(tempsignal.diff()).sum(axis=1).mean()


def calculate_carry(p1, p2, fnd1=None, fnd2=None):
    if (fnd1 is not None) and (fnd2 is not None):
        numberofdays = (fnd1 - fnd2) / pd.Timedelta(days=1)
        toreturn = (np.log(p1) - np.log(p2)) / numberofdays
    else:
        toreturn = (np.log(p1) - np.log(p2))
    toreturn.replace([np.inf, -np.inf], np.nan)
    return toreturn


def print_stat(tempsignal, assetreturn,
               anualized_factor, numberofdays2trade=252, ind=True,
               items=None,
               ):
    if items is None:
        items = ['profit_per_trade', 'std', 'std_daily', 'sharpe', 'sharpe_daily', 'turnover']
    if 'profit_per_trade' in items:
        print('profit_per_trade')
        print(profit_pertrade(tempsignal=tempsignal, assetreturn=assetreturn, ind=ind))
    if 'std' in items:
        print('std')
        print(annualized_std(tempsignal, assetreturn, factor=anualized_factor, ind=ind))
    if 'std_daily' in items:
        print('std_daily')
        print(annualized_std_daily(tempsignal, assetreturn, factor=numberofdays2trade, ind=ind))
    if 'sharpe' in items:
        print('sharpe')
        print(sharpe_ratio(tempsignal, assetreturn, factor=anualized_factor, ind=ind))
    if 'sharpe_daily' in items:
        print('sharpe_daily')
        print(sharpe_ratio_daily(tempsignal, assetreturn, factor=numberofdays2trade, ind=ind))
    if 'turnover' in items:
        print('turnover')
        print(turnover(tempsignal=tempsignal, ind=ind))


def pcaratio(returnmatrix, eachstep, debug=False):
    toreturn = returnmatrix.iloc[:, 0].copy()
    toreturn.name = 'pcaratio'
    toreturn = (toreturn * np.NaN).copy()
    pca = PCA()
    for i in range(eachstep, toreturn.shape[0] + 1):
        X = returnmatrix.iloc[range(i - eachstep, i), :]
        try:
            pca.fit(X.dropna(axis=0, how='all'))
            toreturn.loc[X.index[-1]] = pca.explained_variance_ratio_[0]
        except:
            if debug:
                print(X)
            toreturn.loc[X.index[-1]] = np.NaN
    return toreturn.to_frame().copy()


def readlocaldata(datadir, ticker, timestampadjust='1min'):
    rawdata = pd.read_csv(datadir + '\\' + ticker + '.csv\\' + ticker + '.txt', header=None)
    rawdata.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    toreturn = rawdata.loc[:, ['close', 'high', 'low', 'open', 'volume']]
    toreturn.index = pd.to_datetime(rawdata['datetime'].values)
    toreturn['openinterest'] = 0.0
    toreturn.index = toreturn.index + pd.Timedelta(timestampadjust)
    toreturn.fillna(0, inplace=True)
    return toreturn


def signalround(tempsignal, precision=0.01, mul=1.0):
    tempsignal = tempsignal * mul
    return round(np.abs(tempsignal) / precision) * precision * np.sign(tempsignal)


def arrayCopy(src, srcPos, dest, destPos, length):
    if (destPos + length) > len(dest):
        print('in arraycopy: (destPos + length) > len(dest)')
        return
    elif (srcPos + length) > len(src):
        print('in arraycopy: (srcPos + length) > len(src)')
        return
    else:
        dest[destPos: (destPos + length)] = src[srcPos: (srcPos + length)].copy()


def vectorshift(v1, shift, daystart, dayend, Time_points_number):
    to_return = v1.shift(shift)
    for day in range(daystart, dayend + 1):
        for i in range(0, shift):
            to_return[i + (day - daystart) * Time_points_number] = np.nan
    return to_return


def drawdowncontrol(momsignal, fonbday1, normalize_lookbackperiod=120, voltarget=0.15, thelag=2):
    cumpnl = (momsignal.shift(thelag) * fonbday1).fillna(0).sum(axis=1).cumsum()
    Roll_Max = cumpnl.rolling(normalize_lookbackperiod, min_periods=1).max()
    Daily_Drawdown = (Roll_Max - cumpnl) / voltarget
    drawdowncontrol = pd.Series(2.0 * (1 - norm.cdf(Daily_Drawdown)), index=Daily_Drawdown.index)
    # print(drawdowncontrol)
    return momsignal.mul(drawdowncontrol, axis=0)


def pathinsert(path, toinsert):
    if isinstance(path, str):
        toreturn = toinsert + path
    else:
        thename = toinsert + path.name
        theparent = path.parent
        toreturn = theparent / thename
    return toreturn
