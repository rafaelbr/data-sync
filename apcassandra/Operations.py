#!/usr/bin/python
# -*- coding: utf-8-*-

from apcassandra import CassandraClient, Configuration
import logging
import uuid
import datetime

class Operations:
	statement = None
	columns = [];
	client = None

	log = logging.getLogger()

	def __init__(self):
		self.client = CassandraClient.CassandraClient();


	def initCassandra(self):
		"Initiate a connection between client and Cassandra cluster"
		self.client.connect()

	def closeConnection(self):
		"Close and cleanup connection between Cassandra"
		self.client.close()

	def inquiryTable(self):
		"Prepare update statement to be reused"
		update_columns = ""
		#inquiry columns from table and make a list
		for column in self.client.metadata.keyspaces[Configuration["keyspace"]].tables[Configuration['table']].columns:
			cdata = (self.client.metadata.keyspaces[Configuration["keyspace"]].tables[Configuration['table']].columns[column].name, self.client.metadata.keyspaces[Configuration["keyspace"]].tables[Configuration['table']].columns[column].typestring)
			self.columns.append(cdata)
			if (cdata[1] != 'uuid'):
				update_columns += " " + cdata[0] + " = ?,"
			else:
				id_column = cdata

		update_columns = update_columns[:-1]
		command = "UPDATE " + Configuration['table'] + " SET" + update_columns + " WHERE " + id_column[0] + " = ?"
		self.statement = self.client.session.prepare(command)
		
	def updateData(self, data):
		"Update data into database. Data must be a dictionary"
		data['timestamp'] = datetime.datetime.today()
		bind_data = []
		for column in self.columns:
			if column[1] != 'uuid':
				if data[column[0]]:
					bind_data.append(data[column[0]])
				else:
					bind_data.append(None)
		bind_data.append(data[self.columns[0][0]])
		
		#make update
		try:
			stmt = self.statement.bind(bind_data)
			return self.client.session.execute(stmt)
		except:
			log.info("Error while updating, ignoring line...")

	def retrieveAllData(self):
		"Retrieve all data from table"
		result = []
		query = "SELECT * FROM " + Configuration['table']
		rs = self.client.session.execute(query)
		for row in rs:
			result.append(row.__dict__)

		return result

	def filterByTimestamp(self, timestamp):
		"Make a full table scan and filter by timestamp"
		data = []
		query = "SELECT * FROM " + Configuration['table']
		rs = self.client.session.execute(query)
		for row in rs:
			if (row.timestamp < timestamp):
				continue
			data.append(row.__dict__)

		return data

		
