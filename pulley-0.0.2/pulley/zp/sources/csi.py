import os
import posixpath
import datetime
import operator
import glob
import numpy as np
from pulley.calendar import date_utils

csi_home = os.getenv('CSI_HOME', '')

DTYPE = [('symu', 'S30'),
         ('exchange', 'S30'),
         ('date', 'S30'),
         ('op', 'f'),
         ('hi', 'f'),
         ('lo', 'f'),
         ('cl', 'f'),
         ('vol', 'f')]

"""
Import list of CSI prices from raw CSV files. Files must be of the format
     TICKER, EXCHANGE, DATE (yyyy-mm-dd), OPEN, HIGH, LOW, CLOSE, VOLUME
For use with Zipline.
"""
def get_data(tickers, tBeg, tEnd, include_open=True, portfolio='ETFs'):

    iBeg = date_utils.datetime2iDate(tBeg)
    iEnd = date_utils.datetime2iDate(tEnd)
    trading_dates = date_utils.nyseDates(iBeg, iEnd)

    sPortDir = posixpath.join(csi_home, portfolio)
    vsFiles = glob.glob(posixpath.join(sPortDir, '*.CSV'))

    output = []
    
    for tkr in tickers:
        idxFile = []
        for i, sFile in enumerate(vsFiles):
            if '%s_' % tkr in sFile:
                idxFile.append(i)
        if len(idxFile) == 0:
            raise Exception('Cannot find file for ticker: %s (looking in %s)' % (tkr, sPortDir))
        if len(idxFile) > 1:
            raise Exception('Multiple files found for ticker: %s' % tkr)

        csvPath = vsFiles[idxFile[0]]

        bars = np.genfromtxt(csvPath, delimiter=',', skip_header=False, dtype=DTYPE)

        for bar in bars:
            tkr = bar[0]
            sDate = bar[2]
            nOpen = bar[3]
            nClose = bar[6]
            nVolume = bar[7]

            dt = datetime.datetime.strptime(sDate, '%Y-%m-%d')

            if dt.date() >= tBeg.date() and dt.date() <= tEnd.date():
                if include_open:
                    output.append([dt.replace(hour=9, minute=30), tkr, nOpen, nVolume])
                output.append([dt.replace(hour=16, minute=0), tkr, nClose, nVolume])
                
    return sorted(output, key=operator.itemgetter(0,1))
