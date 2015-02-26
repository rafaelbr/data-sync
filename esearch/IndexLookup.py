from esearch import Configuration, Client
from elasticsearch import Elasticsearch, ConnectionError
import logging

log = logging.getLogger()
log.setLevel(Configuration['log_level'])

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
