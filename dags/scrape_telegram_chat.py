import json
import uuid
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
from airflow.utils.task_group import TaskGroup

from include.filesystem import get_fs
from include.telegram.telegram_scraper import scrape_messages, create_chat_archive, list_chat_archives, list_file_sources
from include.elasticsearch.elasticsearch import bulk_insert, query_index

import re
import yaml
import pendulum
import pandas as pd
import dask.dataframe as dd
import subprocess
import yara
import tempfile
import shutil


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
    download_handlers = config.get("download_handlers", [])

    @dag(dag_id=dag_id, dagrun_timeout=dagrun_timeout, dag_display_name=dag_name, description=dag_description, schedule=schedule, start_date=start_date, catchup=catchup, tags=tags, default_args=default_args)
    def scrape_telegram_chat():

        @task()
        def fetch_and_archive_messages(**kwargs):

            logical_date = kwargs['logical_date']
            previous_task_date = logical_date.subtract(days=1)

            # Get previously downloaded files for this dag run
            chat_files_df = query_index("file_sources", {
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
            })

            # Scrape messages from the Telegram chat
            try:

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

                    source_files = []
                    download_hashes = chat_files_df["file_hash"].unique()
                    for hash in download_hashes:
                        source_files.extend(list_file_sources(hash=hash, start_date=previous_task_date, end_date=logical_date))
                    print(f"Found {len(source_files)} source files matching the criteria.")

                    return {
                        "parquet_files": parquet_files,
                        "source_files": source_files,
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
            bulk_insert(df, index_name="telegram_messages", id_field="_id")

            # Save file sources to Elasticsearch
            if source_files:
                source_json_list = []
                for f in source_files:
                    with fs.open(f, "r") as fp:
                        source_file_content = json.load(fp)
                        source_json_list.append(pd.DataFrame([source_file_content]))
                sources_df = pd.concat(source_json_list, ignore_index=True)

                # Add the _id field for Elasticsearch
                sources_df["_id"] = sources_df["source_data"].map(lambda d: f"telegram_message_{d['file_hash']}_{d['chat_type']}_{d['chat_id']}_{d['message_id']}")

                # Insert into Elasticsearch
                bulk_insert(sources_df, index_name="file_sources", id_field="_id")

            return data
                
        


        def process_file_with_handlers(file_path, file_hash, fs, download_handlers):

            # Create a temporary directory for the file to be downloaded
            base_name = os.path.basename(file_path)
            tmp_dir = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4().hex}")
            os.makedirs(tmp_dir, exist_ok=True)
            local_tmp_path = os.path.join(tmp_dir, base_name)

            # Copy the file to the temp location
            with fs.open(file_path, "rb") as src, open(local_tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            try:
                for handler in download_handlers:

                    process_uuid = uuid.uuid4().hex
                    yara_paths = handler.get("yara")
                    run_cmd = handler.get("run")

                    # --- YARA Matching ---
                    if yara_paths:
                        if isinstance(yara_paths, str):
                            yara_paths = [yara_paths]

                        try:
                            # Compile multiple rules at once
                            file_map = {
                                f"rule_{i}": f"./include/yara/{yp.lstrip('/')}"
                                for i, yp in enumerate(yara_paths)
                            }
                            rules = yara.compile(filepaths=file_map, externals={"filename": ""})

                            def console_cb(msg):
                                print("[YARA console]", msg)

                            matches = rules.match(filepath=local_tmp_path, externals={"filename": os.path.basename(file_path)}, console_callback=console_cb)

                            # print matches
                            print(f"matches: {matches} for {os.path.basename(local_tmp_path)}")


                            if not matches:  # no rule triggered
                                continue
                        except Exception as e:
                            print(f"[!] Failed to compile/run YARA rules {yara_paths}: {e}")
                            continue

                    # --- Run Command ---
                    try:

                        # Create temp file for output_path
                        output_path = os.path.join(tempfile.gettempdir(), f"{process_uuid}-output")
                        os.makedirs(output_path, exist_ok=True)

                        vars = {
                            "file_path": local_tmp_path,
                            "file_hash": file_hash,
                            "output_path": output_path,
                            "uuid": process_uuid
                        }

                        # replace variables in the command
                        for key, value in vars.items():
                            run_cmd = run_cmd.replace(f"${key}", value)

                        print(f"[+] Running handler: {run_cmd}")
                        subprocess.run(run_cmd, shell=True, check=True, stdout=sys.stdout, stderr=sys.stderr)
                        print("[✓] Handler executed successfully.")
                        return output_path
                    except subprocess.CalledProcessError as e:
                        print(f"[!] Handler failed: {e}. Trying next...")

                print("[✗] No handler succeeded.")
                raise Exception("All handlers failed.")


            finally:
                # Clean up temporary file
                try:
                    shutil.rmtree(tmp_dir)
                except OSError as e:
                    print(f"[!] Failed to delete temp folder {tmp_dir}: {e}")

            


        # If download handlers exist, define tasks to handle files

        if download_handlers:

            @task()
            def get_file_paths(data):
                logical_date = data["logical_date"]
                previous_task_date = data["previous_task_date"]

                chat_files_df = query_index("file_sources", {
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
                })

                # if chat_files_df has no rows, skip airflow task
                if chat_files_df.shape[0] == 0:
                    raise AirflowSkipException("Skipping task: no files to process.")

                # build s3 storage locations
                storage_location = os.getenv('STORAGE_LOCATION', 's3://tietl')
                chat_files_df['file_dir'] = chat_files_df.apply(lambda row: f"{storage_location.rstrip('/')}/telegram_downloads/{row['file_hash']}/file", axis=1)

                # map dirs to file paths with file_hash
                fs = get_fs()
                unique_file_tuples = []
                for _, row in chat_files_df.iterrows():
                    dir_path = row['file_dir']
                    file_hash = row['file_hash']
                    files = fs.ls(dir_path)
                    file_name = os.path.basename(files[0]) if files else ""
                    unique_file_tuples.append((file_hash, f"{dir_path}/{file_name}"))

                # remove duplicates
                unique_file_tuples = list(set(unique_file_tuples))

                return unique_file_tuples

            @task()
            def handle_file(file_tuple):

                file_hash, file_path = file_tuple

                print(f"Handling {file_path} with handlers {download_handlers}")

                fs = get_fs()

                processed_path = process_file_with_handlers(file_path, file_hash, fs, download_handlers)

                return processed_path


            @task()
            def save_results(output_path: str):
                print(f"Saving results from {output_path}")
                
                # Iterate over all subfolders in output_path
                for folder_name in os.listdir(output_path):
                    folder_path = os.path.join(output_path, folder_name)
                    
                    if os.path.isdir(folder_path):
                        index_name = folder_name  # Set index_name to folder name
                        
                        # Read all parquet files in the folder into a Dask DataFrame
                        df = dd.read_parquet(folder_path, engine="pyarrow")

                        # Create an es_save_date column if it doesn't exist yet
                        if "es_save_date" not in df.columns:
                            df["es_save_date"] = pendulum.now("UTC").to_iso8601_string()

                        # Call the bulk_insert_dask function
                        bulk_insert(df, index_name=index_name, id_field="_id")

                shutil.rmtree(output_path, ignore_errors=True)


        # DAG flow
        archive_data = fetch_and_archive_messages()
        save_data = save_to_elasticsearch(archive_data)
        

        # If download handlers exist, dynamically map tasks
        if download_handlers:
            file_paths = get_file_paths(save_data)
            processed_paths = handle_file.expand(file_tuple=file_paths)
            save_results.expand(output_path=processed_paths)

    # Instantiate the DAG
    return scrape_telegram_chat()



# ---- Load YAMLs and register DAGs ----
for file in os.listdir(CONFIG_DIR):
    if file.endswith(".yaml") or file.endswith(".yml"):
        with open(os.path.join(CONFIG_DIR, file), "r") as f:
            yaml_content = yaml.safe_load(f)
            for dag_id, dag_config in yaml_content.items():
                globals()[dag_id] = create_telegram_dag(dag_id, dag_config)