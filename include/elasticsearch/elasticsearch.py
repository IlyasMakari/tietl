import json
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
import os


_es_instance = None

def _get_es_instance():
    global _es_instance
    if _es_instance is None:
        es_host = os.getenv('ELASTICSEARCH_HOST')
        es_user = os.getenv('ELASTICSEARCH_USER', None)
        es_password = os.getenv('ELASTICSEARCH_PASSWORD', None)
        _es_instance = Elasticsearch(
            hosts=[es_host],
            http_auth=(es_user, es_password) if es_user and es_password else None
        )
        if not _es_instance.ping():
            raise ConnectionError("Could not connect to Elasticsearch")
        print("Connected to Elasticsearch")
    return _es_instance

# ---------------------------
# Public functions
# ---------------------------

def create_index_if_not_exists(index_name: str, mapping_file: str):
    """Create an Elasticsearch index from a mapping JSON file"""
    es = _get_es_instance()

    if not es.indices.exists(index=index_name):
        with open(mapping_file, "r") as f:
            mapping = json.load(f)
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created")
    else:
        print(f"Index '{index_name}' already exists")

def bulk_insert(df: pd.DataFrame, index_name: str, id_field="_id"):
    """Bulk insert a Pandas DataFrame into Elasticsearch"""
    es = _get_es_instance()

    # Convert DataFrame to list of actions
    actions = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        row_dict.pop(id_field, None)  # Remove id field from source
        actions.append({"_index": index_name, "_id": row[id_field], "_source": row_dict})

    try:
        helpers.bulk(es, actions)
    except BulkIndexError as e:
        # e.errors is a list of failed items
        for item in e.errors:
            for op_type, details in item.items():
                doc_id = details.get('_id')
                reason = details.get('error', {}).get('reason')
                print(f"Failed doc ID {doc_id}: {reason}")
    print(f"{len(actions)} records inserted into '{index_name}'")