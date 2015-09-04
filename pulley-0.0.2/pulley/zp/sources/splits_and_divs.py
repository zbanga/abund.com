"""
Splits and dividend utils for Zipline.
"""

from datetime import datetime, timedelta
import pytz
import pandas as pd

from zipline.gens.utils import hash_args
from zipline.sources.data_source import DataSource
from zipline.protocol import DATASOURCE_TYPE
from zipline.utils.factory import create_split
from zipline.utils.factory import create_dividend


class CsvSplitSource(DataSource):

    sids = []
    
    def __init__(self, fname):
        self.fname = fname
        self.arg_string = hash_args(fname)
        self._raw_data = None
        
    @property
    def mapping(self):
        return {'sid':   (lambda x: x, 'sid'),
                'ratio': (lambda x: x, 'ratio'), #float 
                'dt':    (lambda x: x, 'dt')}

    @property
    def instance_hash(self):
        return self.arg_string

    def raw_data_gen(self):
        with open(self.fname, 'r') as f:
            for line in f:
                toks = line.split(',')
                '''
                Note:
                In create_split(sid, ratio, dt), the input dt is zeroed out to the day, ie)
                    dt.replace(hour=0, minute=0, second=0, microsecond=0).    
                <TODO> When exactly do these split events get fired, on open or on close?
                '''
                event = create_split(toks[0],
                                     float(toks[-1].replace('\n','')),
                                     pytz.utc.localize(datetime.strptime(str(toks[1]), '%Y%m%d')))
                yield event

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data

    @property
    def event_type(self):
        return DATASOURCE_TYPE.SPLIT



class CsvDividendSource(DataSource):

    sids = []
    
    def __init__(self, fname):
        self.fname = fname
        
        # Hash_value for downstream sorting.
        self.arg_string = hash_args(fname)

        self._raw_data = None
        
    @property
    def mapping(self):
        return {
            'sid':          (lambda x: x, 'sid'),
            'gross_amount': (lambda x: x, 'gross_amount'),
            'net_amount':   (lambda x: x, 'net_amount'),
            'dt':           (lambda x: x, 'dt'),
            'ex_date':      (lambda x: x, 'ex_date'),
            'pay_date':     (lambda x: x, 'pay_date'),
        }

    @property
    def instance_hash(self):
        return self.arg_string

    def raw_data_gen(self):
        with open(self.fname, 'r') as f:
            for line in f:
                toks = line.split(',')
                sid = toks[0]
                tDay = pytz.utc.localize(datetime.strptime(str(toks[1]), '%Y%m%d'))
                nAmt = float(toks[2].replace('\n',''))

                # Set ex_date to the data's datetime.
                # Move the declared date arbitrarily one day back from ex_date.
                # Move the payment date arbitrarily one day forward from ex_date.
                ex_date = tDay
                declared_date = ex_date - timedelta(days=1)
                pay_date = ex_date + timedelta(days=1)
                event = create_dividend(sid, nAmt, tDay, tDay, tDay)
                yield event

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data

    @property
    def event_type(self):
        return DATASOURCE_TYPE.DIVIDEND        

'''
Output:
[['EEM', 20050609, 3.0, 1.0, 3.0],...]
'''
def load_split_csv(fname):
    splits = []
    with open(fname, 'r') as f:
        for line in f:
            toks = line.split(',')
            splits.append([toks[0], int(toks[1]),
                           float(toks[2]), float(toks[3]),
                           float(toks[4])])
    # sort by date
    splits.sort(key=lambda x: float(x[1]))
    return splits

'''
Output:
[['TLT', 20080701, 0.332],...]
'''
def load_div_csv(fname):
    divs = []
    with open(fname, 'r') as f:
        for line in f:
            toks = line.split(',')
            divs.append([toks[0], int(toks[1]), float(toks[2])])
    # sort by date
    divs.sort(key=lambda x: float(x[1]))
    return divs
