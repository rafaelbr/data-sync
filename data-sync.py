from esearch import Configuration, IndexLookup
from apcassandra import Operations
from daemon import runner
import time
import datetime
import logging
import atexit


class DataSync:


	def __init__(self):
		self.stdin_path = '/dev/null'
		self.stdout_path = '/dev/tty'
		self.stderr_path = '/dev/tty'
		self.pidfile_path =  '/var/run/datasync.pid'
		self.pidfile_timeout = 5
		self.op = Operations.Operations()

	def run(self):
		self.op.initCassandra()
		log.info("Datasync started at " + datetime.datetime.today().ctime())
		log.info("Gethering database info...")
		self.op.initCassandra()
		self.op.inquiryTable()
		self.op.closeConnection()
		self.last_timestamp = datetime.datetime.utcfromtimestamp(time.time())
		log.info("Waiting for syncronization process...")
		while True:
			time.sleep(int(Configuration['update_time']) * 60)
			log.info("Executing syncronization..")
			self.op.initCassandra()
			self.compareData()
			for row in self.cassandra_data:
				IndexLookup.updateData(row)

			for row in self.es_data:
				self.op.updateData(row)
			log.info("Done syncronization! Saving current timestamp and wait...")
			self.last_timestamp = datetime.datetime.utcfromtimestamp(time.time())
			self.op.closeConnection()

	def compareData(self):
		"Compare data between the lists and correct them with the latest record"

		self.cassandra_data = self.op.filterByTimestamp(self.last_timestamp)
		self.es_data = IndexLookup.filterByTimestamp(self.last_timestamp)

		#strip ids from lists - this can be expansive
		ids_cassandra = []
		for row in self.cassandra_data:
			ids_cassandra.append(row['id'])
		log.info("Found in cassandra: " + ids_cassandra.__str__())
		ids_es = []
		for row in self.es_data:
			ids_es.append(row['id'])
		log.info("Found in es: " + ids_es.__str__())

		#get intersection
		ids = [x for x in ids_es if x in ids_cassandra]
		log.info("Conflict found in: " + ids.__str__())
		for id in ids:
			cassandra_row = [x for x in self.cassandra_data if x['id'] == id]
			es_row = [y for y in self.es_data if y['id'] == id]
			log.info("id " + id.__str__() + " es:" + es_row[0]['timestamp'].__str__() + " cassandra: "+ cassandra_row[0]['timestamp'].__str__())
			if (cassandra_row[0]['timestamp'] < es_row[0]['timestamp']):
				self.cassandra_data.remove(cassandra_row[0])
				log.info("ES has updated info")
			else:
				if (cassandra_row[0]['timestamp'] > es_row[0]['timestamp']):
					self.es_data.remove(es_row[0])
					log.info("Cassandra has updated info")


app = DataSync()

log = logging.getLogger()
log.setLevel(Configuration['log_level'])
handler = logging.FileHandler('datasync.log')
log.addHandler(handler)
daemon_runner = runner.DaemonRunner(app)
#This ensures that the logger file handle does not get closed during daemonization
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()



