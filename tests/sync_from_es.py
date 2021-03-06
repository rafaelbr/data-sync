#!/usr/bin/python
# -*- coding: utf-8-*-

from esearch import IndexLookup
from apcassandra import Configuration, Operations

op = Operations.Operations()
op.initCassandra()
op.inquiryTable()

for data in IndexLookup.retrieveAll():
	op.updateData(data)

op.closeConnection()