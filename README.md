# Data Sync - Cassandra/ElasticSearch syncronizer

A daemon which synchronize a table from Apache Cassandra and ElasticSearch

##Description

This project synchronizes data between Cassandra database and ElasticSearch. For now, only one table is synchronized. It runs within a configurable period. It is bidirectional, so all data at both sides is replicated. If a conflict is found, only the newer data is replicated.

##How it works?

When the daemon starts, it saves the current time and wait for the synchronization process. When the synchronization starts, all data changed after the time saved is retrieved and compared. If it finds a conflict, the timestamp of both data is compared and only the newer is synchronized. The current time is saved and the daemon waits for the next synchronization.

###Current limitations

Data in Cassandra is not easily filtered. Because the need of index in queries, only primary index was entitled to be filtered. But we need timestamp to be updated every time so it cannot have an primary index. Because of this, a full table scan is made in every synchronization. This could degradate the performance when the data grow up.

Every time a record is inserted or updated (upserted), we need to update the timestamp with the current time. Doing so will ensure the daemon will get the data and synchronizes it.

##Requirements

For now, the table in Cassandra must have two required fields. An ID field (named id of type uuid) and a timestamp field. This is used to ensure all newer data is synchronized between data sources.

Some modules are required for daemon to work:
* python-daemon
* elasticsearch
* cassandra

## Usage

Simple run python data-sync.py start|stop|restart to control the daemon. The config.ini has the configuration parameters for daemon, i.e., the update time, connection info for cassandra and elasticsearch and the table/index to lookup and sync.
