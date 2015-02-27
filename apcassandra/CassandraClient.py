from ConfigParser import ConfigParser
from cassandra.cluster import Cluster
import logging
import os

class CassandraClient:
	"Cassandra connection client"
	session = None
	cluster = None
	metadata = None
	configuration = {}

	log = logging.getLogger()

	def __init__(self):
		#retrive configuration parameter
		Parser = ConfigParser()
		path = os.path.dirname(__file__)
		path = os.path.split(path)[0]
		Parser.read(path + "/config.ini")
		for option in Parser.options("Cassandra"):
			try:
				self.configuration[option] = Parser.get("Cassandra", option)
			except:
				self.configuration[option] = None
		for option in Parser.options("Global"):
			try:
				self.configuration[option] = Parser.get("Global", option)
			except:
				self.configuration[option] = None

		#init database assets
		self.cluster = Cluster([self.configuration['cluster_address']])
		self.metadata = self.cluster.metadata

	def connect(self):
		"Init connection and create session"
		#init connection, must handle the exception
		self.session = self.cluster.connect(self.configuration['keyspace'])
		self.log.info("Connection opened")

	def close(self): 
		"Close session and cluster"
		self.session.cluster.shutdown()
		self.session.shutdown()
		self.log.info("Connection closed")

	def executeQuery(self, query): 
		"Execute string query directly"
		return self.session.execute(query)

	def executeStatement(self, query, params):
		"Prepare a query, bind it and execute. Params must be a list"
		stmt = self.session.prepare(query)
		stmt.bind(params)
		return self.session.execute(stmt)

