import ConfigParser
import os

from elasticsearch import Elasticsearch

#load configuration
Parser = ConfigParser.ConfigParser()
path = os.path.dirname(__file__)
path = os.path.split(path)[0]
Parser.read(path + "/config.ini")
Configuration = {}
for option in Parser.options("ElasticSearch"):
	try:
		Configuration[option] = Parser.get("ElasticSearch", option)
	except:
		Configuration[option] = None
for option in Parser.options("Global"):
	try:
		Configuration[option] = Parser.get("Global", option)
	except:
		Configuration[option] = None


#create connection
Client = Elasticsearch(Configuration['cluster_address'])