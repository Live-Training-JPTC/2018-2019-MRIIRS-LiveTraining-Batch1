from elasticsearch import Elasticsearch
from pymongo import ConnectionFailure
from pymongo import MongoClient
import requests
import json

#A function to setup a connection between elastic search and python
def Create_connection_es():
    es = None
    es = Elasticsearch([{'host':'localhost', 'port':9200}])
    if es.ping():
        print("Connected")
    else:
        print("Could not establish a connection")
    return es

# A function to setup a connection between mongodb and python
def Create_connection_mongo():
    mdb = None
    mdb = MongoClient('mongodb://localhost:27017')
    return mdb

#A function to extract data from elastic search
def Extract_data(es_object):
    res = es.search(index="company", doc_type="employees", body={"query": {"match_all": {}}})
    print("%d documents found" % res['hits']['total'])
    data={}
    for doc in res['hits']['hits']:
        dump=json.dumps(doc['_source'])
        data[doc['_id']]=json.loads(dump)
    for id in data:
	       for att in data[id]:
		             print att,":",data[id][att]
    return data

es = Create_connection_es()
Extract_data(es)
