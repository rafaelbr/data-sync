#!/usr/bin/python
# -*- coding: utf-8-*-

from esearch import IndexLookup
from apcassandra import Configuration, Operations

op = Operations.Operations()
op.initCassandra()
op.inquiryTable()

for row in op.retrieveAllData():
	IndexLookup.updateData(row)

op.closeConnection()