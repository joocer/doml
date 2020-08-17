# doml
DataOps Monitoring Library

A set of methods to assist with monitoring DataOps pipelines.

Initial version is to record and alert on significant changes to the number of records in a dataset.

~~~
from dataops import DOML

# create the DOML object, load the schema and filename
doml = DOML(["records", "bytes"], "sink")

# record event in the logs
doml.record_event({ "records": 10, "bytes": 1000 })

# test the 'records' valye in the logs, using the last 10 values
print(doml.test_log('records', 10))
~~~

