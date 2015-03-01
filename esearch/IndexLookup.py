from esearch import Configuration, Client
from elasticsearch import Elasticsearch, ConnectionError
import logging
import uuid
import datetime

log = logging.getLogger()

def retrieve(id):
	"Retrieve record given its id"
	try:
		result = Client.get(index=Configuration['index'], doc_type=Configuration['lookup_type'], id=id)
	except ConnectionError:
		log.info("Exception trying to access ElasticSearch cluster")
	else:
		return result["_source"]

def retrieveAll():
	"Retrieve all records from index"
	records = []
	query = { "query": {"match_all": {}}, "filter": {"type": {"value": Configuration['lookup_type']}}}
	try:
		result = Client.search(index=Configuration['index'], body=query)
	except ConnectionError:
		log.info("Exception trying to access ElasticSearch cluster")
	else:
		for hit in result['hits']['hits']:
			record = {}
			record['id'] = hit["_id"]
			for field in hit['_source']:
				record[field] = hit['_source'][field]
			records.append(record)
		return records

def filterByTimestamp(timestamp):
	"Retrieve all records from index filtering by timestamp"
	records = []
	query = {"fields":["_timestamp","_source"],"query":{"filtered":{"filter":{"range":{"_timestamp":{"gt":timestamp}}}}}}
	try:
		result = Client.search(index=Configuration['index'], body=query)
	except ConnectionError:
		log.error("Exception trying to access ElasticSearch cluster")
	else:
		for hit in result['hits']['hits']:
			record = {}
			record['id'] = uuid.UUID(hit["_id"])
			record['timestamp'] = datetime.datetime.utcfromtimestamp(hit['fields']['_timestamp']/1000)
			for field in hit['_source']:
				record[field] = hit['_source'][field]
			records.append(record)
		return records

def updateData(data):
	"Update record into ES Server. Data must be a dictionary"
	id = data['id']
	del data['id']
	try:
		Client.index(index=Configuration['index'], doc_type=Configuration['lookup_type'], id=id, body=data)
	except ConnectionError:
		log.error("Exception trying to access ElasticSearch cluster")

