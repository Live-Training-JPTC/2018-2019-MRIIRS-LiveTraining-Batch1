from elasticsearch import Elasticsearch
import requests
import json

def Create_connection():
    es = None
    es = Elasticsearch([{'host':'localhost', 'port':9200}]) 
    if es.ping():
        print("Connected")
    else:
        print("Could not establish a connection")
    return es
def Extract_data(es_object):
    res = es.search(index="company", doc_type="employees", body={"query": {"match_all": {}}})
    print("%d documents found" % res['hits']['total'])
    data={}
    for doc in res['hits']['hits']:
        dump=json.dumps(doc['_source'])
        data[doc['_id']]=json.loads(dump)
    print(data)
es = Create_connection()
Extract_data(es)

