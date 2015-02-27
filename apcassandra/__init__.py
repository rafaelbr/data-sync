from ConfigParser import ConfigParser
import os

#load configuration
Parser = ConfigParser()
path = os.path.dirname(__file__)
path = os.path.split(path)[0]
Parser.read(path + "/config.ini")
Configuration = {}
for option in Parser.options("Cassandra"):
	try:
		Configuration[option] = Parser.get("Cassandra", option)
	except:
		Configuration[option] = None
for option in Parser.options("Global"):
	try:
		Configuration[option] = Parser.get("Global", option)
	except:
		Configuration[option] = None