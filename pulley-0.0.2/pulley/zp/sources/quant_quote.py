import os
import posixpath
import datetime
import operator

import numpy as np

from pulley.calendar import date_utils

quant_quote_home = os.getenv('QUANT_QUOTE_HOME', '')

DTYPE = [('date', 'i'),
         ('time', 'i'),
         ('op', 'f'),
         ('hi', 'f'),
         ('lo', 'f'),
         ('cl', 'f'),
         ('vol', 'f'),
         ('splits', 'f'),
         ('earnings', 'f'),
         ('divs', 'f')]

def make_datetime(iDate, iTime):
    sTime = str(iTime)
    iMinute = int(sTime[-2:]) # last two characters are the minute
    iHour = int(sTime[0:-2]) # leading characters are the hour
    dt = date_utils.iDate2Datetime(iDate)
    return dt.replace(hour=iHour, minute=iMinute, second=0, microsecond=0, tzinfo=None)

def get_data(tickers, tBeg, tEnd, base_dir=quant_quote_home, crop=True):

    iBeg = date_utils.datetime2iDate(tBeg)
    iEnd = date_utils.datetime2iDate(tEnd)
    trading_dates = date_utils.nyseDates(iBeg, iEnd)

    output = []

    for tkr in tickers:
        tkr_lower = tkr.lower()
        tkr_upper = tkr.upper()
        for date in trading_dates:
            fname = posixpath.join(base_dir, 'allstocks_%i' % date, 'table_%s.csv' % tkr_lower)
            bars = np.genfromtxt(fname, delimiter=',', skip_header=False, dtype=DTYPE)
            bars = bars[(bars['time'] >= 931) & (bars['time'] <= 1600)]

            # if bars.shape[0] != 390:
            #     print 'Number of bars != 390: %s on %i has %i bars. [%s, %s]' % \
            #               (tkr_upper, date, bars.shape[0], bars['time'][0], bars['time'][-1])
            
            for bar in bars:
                dt = make_datetime(bar[0], bar[1])
                output.append([dt, tkr, bar[5], bar[6]])
                
    # sort by datetime then ticker for use with Zipline       
    return sorted(output, key=operator.itemgetter(0,1))

