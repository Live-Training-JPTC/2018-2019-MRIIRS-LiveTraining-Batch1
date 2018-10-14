from elasticsearch import Elasticsearch
def Create_connection():
    es=None
    es = Elasticsearch([{'host'='localhost',port=9200}]) 
    if es.ping():
        print "Connected"
    else:
        print "Could not establish a connection"
    return es

