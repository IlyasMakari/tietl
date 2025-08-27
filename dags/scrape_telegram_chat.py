import json
from importlib_metadata import files
import pendulum
from airflow.decorators import dag, task
from pendulum import datetime
from datetime import datetime, timedelta
import asyncio
import os
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import InviteHashExpiredError
import sys

from airflow.exceptions import AirflowSkipException

from include.filesystem import get_fs
from include.telegram.telegram_scraper import scrape_messages, create_chat_archive, list_chat_archives
from include.elasticsearch.elasticsearch import create_index_if_not_exists, bulk_insert, query_index

import re
import yaml
import pendulum
import pandas as pd
# import dask.dataframe as dd


# Path to YAML configs
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "telegram_chats")

def create_telegram_dag(dag_id, config):
    """Factory that returns a DAG for one Telegram chat"""

    schedule = config.get("schedule_interval", "@daily")
    start_date = pendulum.parse(config.get("start_date", "2023-01-01"))
    default_args = config.get("default_args", {"retries": 0})
    chat_info = config.get("chat_info", {})
    chat_entity = chat_info.get("chat_entity", "")
    chat_type = chat_info.get("chat_type", "")
    chat_id = chat_info.get("chat_id", "")
    chat_name = chat_info.get("chat_name", "")
    dag_name = config.get("dag_name", chat_name + " - Telegram Chat Scraper")
    dag_description = config.get("dag_description", "Scrape messages from " + chat_name)
    catchup = config.get("catchup", False)
    tags = config.get("tags", ["telegram", "chatrooms"])
    dagrun_timeout = config.get("dagrun_timeout_minutes", None)
    if dagrun_timeout:
        dagrun_timeout = timedelta(minutes=dagrun_timeout)

    enable_downloads = config.get("enable_downloads", False)
    download_rules = config.get("download_rules", {})

    @dag(dag_id=dag_id, dagrun_timeout=dagrun_timeout, dag_display_name=dag_name, description=dag_description, schedule=schedule, start_date=start_date, catchup=catchup, tags=tags, default_args=default_args)
    def scrape_telegram_chat():

        @task()
        def fetch_and_archive_messages(**kwargs):

            logical_date = kwargs['logical_date']
            previous_task_date = logical_date.subtract(days=1)

            # Scrape messages from the Telegram chat
            try:

                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"source_data.chat_type": chat_type}},
                                {"term": {"source_data.chat_id": chat_id}}
                            ],
                            "filter": [
                                {
                                    "range": {
                                        "source_date": {
                                            "gte": previous_task_date.to_iso8601_string(),
                                            "lte": logical_date.to_iso8601_string()
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }

                chat_files_df = query_index("file_sources", query)

                chat = asyncio.run(scrape_messages(chat_entity, start_time=previous_task_date, end_time=logical_date, fs=get_fs(), download_files=enable_downloads, download_rules=download_rules, existing_downloads=chat_files_df))
                print("Scraping succeeded!")

            except InviteHashExpiredError as e:
                print(f"Error scraping: {e}")
                print("Falling back to archive if available.")
                parquet_files = list_chat_archives(
                    chat_type=chat_type,
                    chat_id=chat_id,
                    start_date=previous_task_date,
                    end_date=logical_date
                )
                print(f"Found {len(parquet_files)} archived files matching the criteria.")
                print(f"Parquet files: {parquet_files}")
                if parquet_files:
                    return {
                        "parquet_files": parquet_files,
                        "logical_date": logical_date,
                        "previous_task_date": previous_task_date
                    }
                else:
                    raise Exception("No scraped data or archive available.")

            # If chat is empty, skip the task
            if not chat.get("chat_history", []):
                raise AirflowSkipException("No messages found in the chat. Skipping task.")
                
            parquet_files = create_chat_archive(chat, fs=get_fs())

            return {
                "parquet_files": parquet_files,
                "source_files": chat.get("source_files", []),
                "logical_date": logical_date,
                "previous_task_date": previous_task_date
            }


        @task()
        def save_to_elasticsearch(data):

            fs = get_fs()

            parquet_files = data["parquet_files"]
            source_files = data["source_files"]
            logical_date = data["logical_date"]
            previous_task_date = data["previous_task_date"]

            df = pd.read_parquet(parquet_files, engine="pyarrow", filesystem=fs)

            # print dataframe info
            print(f"Dataframe length: {len(df)}")

            # Filter dataframe to only include messages within the date range
            df["message_date"] = pd.to_datetime(df["message_date"], utc=True, format='ISO8601')
            df = df[(df["message_date"] >= previous_task_date) & (df["message_date"] <= logical_date)]
            print(f"Dataframe length after filtering date: {len(df)}")

            # Deduplicate based on chat_type, chat_id, id keeping the latest archive_date
            df["archive_date"] = pd.to_datetime(df["archive_date"], utc=True, format='ISO8601')
            df = df.sort_values(by="archive_date", ascending=False)
            latest_archive_idx = df.groupby(["chat_type", "chat_id", "message_id"])["archive_date"].idxmax()
            df = df.loc[latest_archive_idx].reset_index(drop=True)
            print(f"Dataframe length after deduplication: {len(df)}")

            # add save date columns
            df["es_logical_save_date"] = logical_date.to_iso8601_string()
            df["es_save_date"] = pendulum.now("UTC").to_iso8601_string()

            # Add the _id field for Elasticsearch based on chat_type, chat_id, message_id
            df["_id"] = df.apply(lambda row: f"{row['chat_type']}_{row['chat_id']}_{row['message_id']}", axis=1)

            # Fix nan values
            for col in ['fwd_from_id', 'file_size_bytes', 'reply_to_msg_id', 'fwd_from_channel_post']:
                df[col] = df[col].astype('Int64')  # nullable integer type

            # Insert into Elasticsearch
            create_index_if_not_exists(index_name="telegram_messages", mapping_file="/usr/local/airflow/include/elasticsearch/mappings/telegram_messages.json")
            bulk_insert(df, index_name="telegram_messages", id_field="_id")

            # Save file sources to Elasticsearch
            if source_files:
                source_json_list = []
                for f in source_files:
                    with fs.open(f, "r") as fp:
                        data = json.load(fp)
                        source_json_list.append(pd.DataFrame([data]))
                sources_df = pd.concat(source_json_list, ignore_index=True)

                # Add the _id field for Elasticsearch
                sources_df["_id"] = sources_df["source_data"].map(lambda d: f"telegram_message_{d['file_hash']}_{d['chat_type']}_{d['chat_id']}_{d['message_id']}")

                # Insert into Elasticsearch
                create_index_if_not_exists(index_name="file_sources", mapping_file="./include/elasticsearch/mappings/file_sources.json")
                bulk_insert(sources_df, index_name="file_sources", id_field="_id")


        # DAG flow
        archive_data = fetch_and_archive_messages()
        save_to_elasticsearch(archive_data)


    # Instantiate the DAG
    return scrape_telegram_chat()



# ---- Load YAMLs and register DAGs ----
for file in os.listdir(CONFIG_DIR):
    if file.endswith(".yaml") or file.endswith(".yml"):
        with open(os.path.join(CONFIG_DIR, file), "r") as f:
            yaml_content = yaml.safe_load(f)
            for dag_id, dag_config in yaml_content.items():
                globals()[dag_id] = create_telegram_dag(dag_id, dag_config)