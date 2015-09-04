
from zipline.utils.factory import load_bars_from_yahoo

'''
Fetch a Pandas DataPanel of Yahoo finance historical bar data.
'''
def fetch(tickers, tBeg, tEnd, adjusted=False):
    return load_bars_from_yahoo(stocks=tickers, indexes={}, start=tBeg, end=tEnd, adjusted=adjusted)

'''
Convert a Zipline Yahoo fetch into a flat list of date-sorted tuples.
'''
def flatten_panel(panel, include_open=True):
    rows_prices = []
    for t_utc in panel.major_axis:
        dt = t_utc.to_datetime().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        if include_open:
            for tkr in panel.items:
                bar = panel.ix[tkr, t_utc]
                rows_prices.append([dt.replace(hour=9, minute=30), tkr, bar['open'], bar['volume']])
        for tkr in panel.items:
            bar = panel.ix[tkr, t_utc]                
            rows_prices.append([dt.replace(hour=16, minute=0), tkr, bar['close'], bar['volume']])
    return rows_prices

