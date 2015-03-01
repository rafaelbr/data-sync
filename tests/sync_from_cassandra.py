#!/usr/bin/python
# -*- coding: utf-8-*-

from esearch import IndexLookup
from apcassandra import Configuration, Operations
import datetime

op = Operations.Operations()
op.initCassandra()
op.inquiryTable()

for row in IndexLookup.filterByTimestamp(datetime.datetime(2015, 02, 28)):
	print row


op.closeConnection()