'''
This class localizes all database dates to US/EST by default. The query must
have one column selected 'AS sid' and another 'AS dt'.

USAGE:

query = 'SELECT obs_date as dt, ticker as sid, other_field FROM price_table'
rows = cur.fetchall(query)
source = QuerySource(rows, ['dt', 'sid', 'other_field'],  DATASOURCE_TYPE.CUSTOM)
algo.run([source])
'''

import datetime
import decimal

from pytz import utc
from pytz import timezone

from zipline.protocol import Event, DATASOURCE_TYPE
from zipline.gens.utils import hash_args
from zipline.sources.data_source import DataSource

class QuerySource(DataSource):

    sids = [] # new requirement
    
    def __init__(self, rows, cols, datasource_type, time_zone='US/Eastern'):
        self.rows = rows
        self.cols = cols
        self.datasource_type = datasource_type
        
        # These are mandatory for the Zipline DataSource class.
        self.arg_string = hash_args(cols)
        self._raw_data = None
        self.time_zone = timezone(time_zone)
        
    @property
    def mapping(self):
        map_dict = {}
        for col in self.cols:
            map_dict[col] = (lambda x: x, col)
        return map_dict

    @property
    def instance_hash(self):
        return self.arg_string

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()
        return self._raw_data

    @property
    def event_type(self):
        return self.datasource_type

    def raw_data_gen(self):
        for row in self.rows:
            event_dict = {}
            for i, col in enumerate(self.cols):
                row_type = type(row[i])
                if row_type == datetime.datetime:
                    # localize any datetimes
                    dt = row[i]
                    dt = self.time_zone.localize(dt).astimezone(utc)
                    event_dict[col] = dt
                elif row_type == decimal.Decimal:
                    # cast any Decimal types into floats
                    event_dict[col] = float(row[i])                    
                else:
                    event_dict[col] = row[i]
            event_dict['type'] = self.datasource_type
            event = Event(event_dict)
            yield event
