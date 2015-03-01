from ConfigParser import ConfigParser
from cassandra.cluster import Cluster
from apcassandra import Configuration
import logging
import os

class CassandraClient:
	"Cassandra connection client"
	session = None
	cluster = None
	metadata = None
	configuration = Configuration

	def __init__(self):
		self.log = logging.getLogger()

		


	def connect(self):
		"Init connection and create session"
		
		#init database assets
		self.cluster = Cluster([self.configuration['cluster_address']])
		self.metadata = self.cluster.metadata
		#init connection, must handle the exception
		self.session = self.cluster.connect(self.configuration['keyspace'])
		self.log.debug("Connection opened")

	def close(self): 
		"Close session and cluster"
		self.session.cluster.shutdown()
		self.session.shutdown()
		self.log.debug("Connection closed")

	def executeQuery(self, query): 
		"Execute string query directly"
		return self.session.execute(query)

	def executeStatement(self, query, params):
		"Prepare a query, bind it and execute. Params must be a list"
		stmt = self.session.prepare(query)
		stmt.bind(params)
		return self.session.execute(stmt)

