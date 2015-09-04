import os
import posixpath

import numpy as np
import pandas as pd

from datetime import datetime, date, time, timedelta
import dateutil.rrule as dr
import dateutil.relativedelta as drel
from dateutil.relativedelta import relativedelta

CALENDAR_DIR = os.path.dirname(__file__)


'''
Get NYSE business days within range [tBeg,tEnd].
Input dates tBeg and tEnd are integer yyyymmdd format (iDay).
Ouptut dates are the same format.
'''
def nyseDates(tBeg, tEnd):
    vnBD = np.genfromtxt(posixpath.join(CALENDAR_DIR, 'dates_nyse.csv'), delimiter=',', dtype="int")
    vnBD = vnBD[np.nonzero(np.logical_and(vnBD >= tBeg, vnBD <= tEnd))[0]]
    return vnBD

'''
Get all NYSE business days on file.
'''
def nyseDatesAll():
    vnBD = np.genfromtxt(posixpath.join(CALENDAR_DIR, '/dates_nyse.csv'), delimiter=',', dtype="int")
    return vnBD

'''
Get NYSE business days within range [tBeg,tEnd].
Input dates tBeg and tEnd are integer yyyymmdd format.
Output dates are datetime.datetime objects.
'''
def nyseDatesDT(tBeg, tEnd):
    viBD = nyseDates(tBeg, tEnd)
    n = len(viBD)
    out = []
    for iDate in viBD:
        out.append(iDate2Datetime(iDate))
    return out

'''
Get NYSE dates as a Pandas series indexed by python Datetime objects with
iDay entries. Useful for finding previous trading days.
'''
def nyseDatesPD(tBeg, tEnd):
    viBusdays = nyseDates(tBeg, tEnd)
    vdBusdays = nyseDatesDT(tBeg, tEnd)
    return pd.Series(viBusdays, index=vdBusdays)

'''
Input date is Python datetime. Returns true if this is the Third friday of the month.
'''
def isThirdFriday(nDate):
    rr = dr.rrule(dr.MONTHLY, byweekday=drel.FR(3), dtstart=nDate, count=1, cache=True)
    if rr[0] == nDate:
        return True
    else:
        return False

'''
If nDate is a datetime for a third friday of a month, this returns the friday count.
If nDate is NOT a third friday, this returns the negative weekday, ie) -1
'''
def thirdFridayNum(nDate):
    theFirst = nDate + relativedelta(day=1)
    rr = dr.rrule(dr.WEEKLY, byweekday=drel.FR(3), dtstart=theFirst, count=5, cache=True)
    count = 1;
    for fri3 in rr:
        if fri3 == nDate:
            return count
        else:
            count += 1
    if count > 5:
        return -1.0 * nDate.weekday()

    return count

'''
Split datetime into integer date and times.
'''
def datetimeSplit(dt):
    return [int(dt.strftime('%Y%m%d')), int(dt.strftime('%H%M%S'))]

'''
Convert a datetime.datetime into a POSIX timestamp.
'''
def datetime2posix(dt):
    return time2.mktime(dt.timetuple())

'''
Convert POSIX timestamp into Python datetime.
<NOTE> Precision will be lost on millisecond scale.
'''
def posix2datetime(posix):
    return datetime.fromtimestamp(posix)

'''
Convert an interger yyyymmdd (iDay) into a Python datetime.datetime.
'''
def iDate2Datetime(iDate):
    return datetime.strptime(str(iDate), '%Y%m%d')

'''
Convert a Python datetime.datetime into an integer yyyymmdd (iDay)
'''
def datetime2iDate(dt):
    return int(dt.strftime('%Y%m%d'))

'''
Return the integer yyyymmdd (iDay) for the current day.
'''
def iDateNow():
    return datetime2iDate(datetime.now())

'''
Convert Python datetime into Hive format TIMESTAMP
'''
def datetime2hive(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

'''
Convert a string like 'YYYY-MM-DD hh:mm:ss' into a Posix format timestamp.
For converting Hadoop output to PyTables output.
'''
def sql2posix(sqlDate):
    return datetime2posix(datetime.strptime(sqlDate, '%Y-%m-%d %H:%M:%S'))
