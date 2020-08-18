"""
DOML

DataOps Monitoring Library
"""

import datetime
import json
from .helper import reverse_file_read
import timeseries as ts

class DOML(object):
    """
    DataOps Monitoring Library

    This is a proof of concept implementation of DOML which uses files (CSVs).
    """

    def __init__(self, schema, log_sink):
        self._schema = schema
        self._log_sink = log_sink

        if not '.' in self._log_sink:
            self._log_sink = self._log_sink + '.jsonl'


    def record_event(self, event, timestamp=None):
        """
        Records details of an DataOps Event.

        Parameters
            timestamp: the date and time of the event
            event: the details of the event
        """

        if timestamp is None:
            timestamp = datetime.datetime.now()

        # make sure we 
        if type(timestamp).__name__ != 'datetime':
            raise Exception("`timestamp` must be a datetime")
        if type(event).__name__ != 'dict':
            raise Exception("`event` must be a dictionary")

        # pre-convert the timestamp to a string as python json fails
        event['timestamp'] = timestamp.isoformat()

        # convert the row to json lines format and save
        self._save_to_log(json.dumps(event) + '\n')


    def test_log(self, key, test_window = 30, establish_window = 8):
        """
        Tests logs of DataOps Events for anomalies:

        Parameters
            key:
            test_window: 
            establish_window:
        """
        records = self._fetch_logs(test_window)
        timestamps = self._get_column_from_object_array('timestamp', records)
        observations = self._get_column_from_object_array(key, records)
        # set up control chart
        cc = ts.controlchart(establish_window)
        cc.load_from_arrays(timestamps, observations)
        # return violations
        return cc.rules.basic()


    def _get_column_from_object_array(self, column, object_array, condition = True):
        values = [ record.get(column) for record in object_array if condition ]
        return values[::-1]


    def _save_to_log(self, record):
        with open(self._log_sink, "a+") as f:
            f.write(record)


    def _fetch_logs(self, records):
        """
        fetches records from the end of the log file, upto the number specified in the 
        records parameter.

        Assumes (fails unpredictably) that the file is in JSON Lines format
        """
        index = 0
        observations = []
        with open(self._log_sink) as fp:
            for line in reverse_file_read(fp):
                line = line.rstrip("\n")
                if len(line) > 0:
                    if index < records:
                        record = json.loads(line)
                        record['timestamp'] = datetime.datetime.fromisoformat(record.get('timestamp'))
                        observations.insert(0, record)
                        index = index + 1
                    else:
                        return observations
        return observations