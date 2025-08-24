from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(hosts=["http://elasticsearch:9200"])

# Test connection
if es.ping():
    print("Connected to Elasticsearch!")
else:
    print("Connection failed.")


# Example: create an index
es.index(index="test-index", document={"message": "Hello TIETLE!"})
print("Document indexed.")