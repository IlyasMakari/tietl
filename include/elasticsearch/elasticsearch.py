import json
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
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


def bulk_insert(df, index_name: str, id_field="_id"):
    """Bulk insert a Pandas or Dask DataFrame into Elasticsearch"""
    create_index_if_not_exists(
        index_name=index_name,
        mapping_file=f"./include/elasticsearch/mappings/{index_name}.json"
    )

    if isinstance(df, pd.DataFrame):
        return _insert_pandas(df, index_name, id_field)
    elif isinstance(df, dd.DataFrame):
        return _insert_dask(df, index_name, id_field)
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df)}")
    

def _insert_pandas(df: pd.DataFrame, index_name: str, id_field="_id"):
    es = _get_es_instance()
    actions = [
        {"_index": index_name, "_id": row[id_field], "_source": {k: v for k, v in row.items() if k != id_field}}
        for _, row in df.iterrows()
    ]
    try:
        helpers.bulk(es, actions)
    except BulkIndexError as e:
        for item in e.errors:
            for _, details in item.items():
                doc_id = details.get('_id')
                reason = details.get('error', {}).get('reason')
                print(f"Failed doc ID {doc_id}: {reason}")
    print(f"{len(actions)} records inserted into '{index_name}'")


def _insert_partition(partition: pd.DataFrame, index_name: str, id_field="_id"):
    es = _get_es_instance()
    actions = (
        {"_index": index_name, "_id": row[id_field], "_source": {k: v for k, v in row.items() if k != id_field}}
        for _, row in partition.iterrows()
    )
    helpers.bulk(es, actions)


def _insert_dask(df: dd.DataFrame, index_name: str, id_field="_id"):
    tasks = [delayed(_insert_partition)(part, index_name, id_field) for part in df.to_delayed()]
    compute(*tasks)
    print(f"Inserted data from Dask DataFrame into '{index_name}'")
    

def query_index(index_name: str, query: dict, size: int = 10000) -> pd.DataFrame:
    """
    Query an Elasticsearch index and return results as a Pandas DataFrame.
    """
    es = _get_es_instance()
    
    try:
        response = es.search(index=index_name, body=query, size=size)
        hits = response.get('hits', {}).get('hits', [])
        if not hits:
            return pd.DataFrame()  # Return empty DataFrame if no results
        # Extract _source and _id for each hit
        data = [{**hit['_source'], '_id': hit['_id']} for hit in hits]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error querying index '{index_name}': {e}")
        return pd.DataFrame()