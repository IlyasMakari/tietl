from telethon import TelegramClient
import os
import sys
from FastTelethonhelper import fast_download
import pendulum
import json
import pandas as pd
import datetime
import fsspec
import re
import hashlib
import shutil

from include.filesystem import get_fs

def create_message_summary(message, chat, chat_entity_id, from_entity, fwd_from_entity, file_hash=None):

    return {

        "chat_title": chat.title,
        "chat_id": chat.id,
        "entity_id": chat_entity_id,
        "chat_type": type(chat).__name__,
        "message_id": message.id,
        "grouped_id": message.grouped_id,
        "is_broadcast": getattr(chat, "broadcast", False),
        "is_megagroup": getattr(chat, "megagroup", False),
        "is_gigagroup": getattr(chat, "gigagroup", False),
        "participants_count": getattr(chat, "participants_count", None),

        "from_type": (
          "Channel" if getattr(message.from_id, "channel_id", None) is not None else
          "User" if getattr(message.from_id, "user_id", None) is not None else
          "Chat" if getattr(message.from_id, "chat_id", None) is not None else
          None
        ),
        "from_id": (
            getattr(message.from_id, "channel_id", None)
            or getattr(message.from_id, "user_id", None)
            or getattr(message.from_id, "chat_id", None)
        ),

        "from_username": getattr(from_entity, "username", None) if from_entity else None,
        "from_first_name": getattr(from_entity, "first_name", None) if from_entity else None,
        "from_last_name": getattr(from_entity, "last_name", None) if from_entity else None,
        "from_channel_title": getattr(from_entity, "title", None) if from_entity else None,

        "sender_id": message.sender_id,
        "message_date": message.date.isoformat(),
        "text": message.text,
        "file_name": getattr(message.file, "name", None) if message.file else None,
        "file_size_bytes": getattr(message.file, "size", None) if message.file else None,
        "file_mime_type": getattr(message.file, "mime_type", None) if message.file else None,
        "file_hash": file_hash,

        "reply_to_msg_id": getattr(message.reply_to, "reply_to_msg_id", None) if message.reply_to else None,

        "fwd_from_type": (
            "Channel" if getattr(getattr(message.fwd_from, "from_id", None), "channel_id", None) is not None else
            "User" if getattr(getattr(message.fwd_from, "from_id", None), "user_id", None) is not None else
            "Chat" if getattr(getattr(message.fwd_from, "from_id", None), "chat_id", None) is not None else
            None
        ),

        "fwd_from_id": (
            getattr(getattr(message.fwd_from, "from_id", None), "channel_id", None)
            or getattr(getattr(message.fwd_from, "from_id", None), "user_id", None)
            or getattr(getattr(message.fwd_from, "from_id", None), "chat_id", None)
        ),

        "fwd_from_username": getattr(fwd_from_entity, "username", None) if fwd_from_entity else None,
        "fwd_from_first_name": getattr(fwd_from_entity, "first_name", None) if fwd_from_entity else None,
        "fwd_from_last_name": getattr(fwd_from_entity, "last_name", None) if fwd_from_entity else None,
        "fwd_from_channel_title": getattr(fwd_from_entity, "title", None) if fwd_from_entity else None,

        "fwd_from_name": getattr(message.fwd_from, "from_name", None),
        "fwd_from_post_author": getattr(message.fwd_from, "post_author", None),
        "fwd_from_date": message.fwd_from.date.isoformat() if getattr(message.fwd_from, "date", None) is not None else None,
        "fwd_from_channel_post": getattr(message.fwd_from, "channel_post", None),

    }



def create_chat_archive(chat, fs, file_location="telegram_chat_archives"):
    df_messages = pd.DataFrame(chat["chat_history"])
    df_messages["archive_date"] = chat["end_time"].to_iso8601_string()

    # Build the S3 file path
    storage_location = os.getenv('STORAGE_LOCATION', 's3://tietl')
    file_name = f"chat_archive_{chat['chat_type']}{chat['chat_id']}_{chat['start_time'].to_iso8601_string()}-{chat['end_time'].to_iso8601_string()}.parquet"
    file_path = f"{storage_location.rstrip('/')}/{file_location.rstrip('/')}/{file_name}"

    # Write directly to S3/MinIO via fsspec
    with fs.open(file_path, "wb") as f:
        df_messages.to_parquet(f, engine="pyarrow", index=False)

    print(f"Chat archive saved to {file_path}")

    return [file_path]



def human_bytes(n):
    if n is None:
        return "?"
    units = ["B","KB","MB","GB","TB","PB"]
    i = 0
    n = float(n)
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.1f} {units[i]}"

def progress_text(done, total):
    pct = (done / total * 100) if total else 0
    s = f"{human_bytes(done)} / {human_bytes(total)}  ({pct:.1f}%)"
    print("\r" + s, end="", flush=True)
    return "⬇️ " + s


def _should_download(message, from_entity, rules):
    if not rules:
        return True

    file = message.file
    text = message.text or ""
    username = getattr(from_entity, "username", None)

    # MIME types
    if "allowed_mime_types" in rules:
        allowed = rules["allowed_mime_types"]
        if isinstance(allowed, str):
            allowed = [allowed]
        if file.mime_type not in allowed:
            return False

    # Filename regex
    if "name_regex" in rules and file.name:
        if not re.search(rules["name_regex"], file.name):
            return False

    # Username
    if "username" in rules and username:
        if username != rules["username"]:
            return False

    # Text regex
    if "text_regex" in rules:
        if not re.search(rules["text_regex"], text):
            return False

    # Size constraints
    if "min_bytes" in rules and file.size:
        if file.size < rules["min_bytes"]:
            return False
    if "max_bytes" in rules and file.size:
        if file.size > rules["max_bytes"]:
            return False

    return True

def sha256sum(filename):
    h = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

async def init_telegram_client():
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    client = TelegramClient(
        "/usr/local/airflow/telethon_sessions/tietl.session",
        api_id,
        api_hash
    )
    await client.start()
    return client


def archive_file(local_path, file_name, file_size, mime_type, file_hash, source_data, archive_date, source_date, download_location="telegram_downloads"):
    fs = get_fs()

    # Build S3 path
    storage_location = os.getenv('STORAGE_LOCATION', 's3://tietl')
    s3_dir = f"{storage_location.rstrip('/')}/{download_location.rstrip('/')}/{file_hash}"
    s3_file_path = f"{s3_dir}/file/{file_name}"
    s3_source_file_path = f"{s3_dir}/source_{archive_date.to_iso8601_string()}.json"

    # Check if already exists in S3
    first_download = False
    if not fs.exists(s3_dir):
        print(f"Uploading to {s3_file_path}")
        with fs.open(s3_file_path, "wb") as f:
            with open(local_path, "rb") as lf:
                shutil.copyfileobj(lf, f)
                first_download = True
    else:
        print(f"Skipping upload, already exists in {s3_dir}")

    source_json = {
        "file_hash": file_hash,
        "file_name": file_name,
        "file_size_bytes": file_size,
        "mime_type": mime_type,
        "source_type": "telegram_message",
        "source_date": source_date.isoformat(),
        "archive_date": archive_date.to_iso8601_string(),
        "source_data": source_data,
        "first_download": first_download
    }

    with fs.open(s3_source_file_path, "w") as f:
        json.dump(source_json, f, indent=2)

    print(f"Uploaded source JSON to {s3_source_file_path}")
    return s3_source_file_path


async def scrape_messages(chat_entity_id, start_time, end_time, fs=None, download_files=False, download_rules=None, existing_downloads=None):
    
    # If downloads true, make sure fs is set
    if download_files and fs is None:
        raise ValueError("File system (fs) must be set when download_files is True")

    client = await init_telegram_client()
    chat = await client.get_entity(chat_entity_id)

    chat_history = []  # store messages for export
    source_files = []
    filtered_existing_downloads = None

    async for message in client.iter_messages(chat, reverse=True, offset_date=start_time):
        if not (start_time <= pendulum.instance(message.date) < end_time):
            continue  # Skip messages outside the desired time range

        try:
            from_entity = await client.get_entity(message.from_id)
        except Exception:
            from_entity = None

        try:
            if message.fwd_from:
              fwd_from_entity = await client.get_entity(message.fwd_from.from_id)
            else:
              fwd_from_entity = None
        except Exception:
            fwd_from_entity = None


        local_path = None
        
        try:
            if (download_files
                and message.file
                and getattr(message.file, "name", None)
                and _should_download(message, from_entity, download_rules)):

                if existing_downloads is not None and not existing_downloads.empty:

                    print("Checking existing downloads for matches...")

                    filtered_existing_downloads = existing_downloads[
                        (existing_downloads['file_name'] == message.file.name) &
                        (existing_downloads['mime_type'] == message.file.mime_type) &
                        (existing_downloads['file_size_bytes'] == message.file.size) &
                        (existing_downloads['source_data'].apply(lambda x: x.get('message_id') == message.id))
                    ]

                if filtered_existing_downloads is not None and not filtered_existing_downloads.empty:
                    print(f"Skipping download. Found existing download for {message.file.name}:")
                else:

                    print(f"Found media '{message.file.name}', starting fast download...")
                    progress_msg = await client.send_message("me", "Starting…")
                    local_path = await fast_download(
                        client=client,
                        msg=message,
                        reply=progress_msg,
                        progress_bar_function=progress_text
                    )
                    print(f"Downloaded to: {local_path}")

                    # Calculate SHA256 hash
                    file_hash = sha256sum(local_path)

                    # Create message summary
                    msg_summary = create_message_summary(message, chat, chat_entity_id, from_entity, fwd_from_entity, file_hash)

                    # Archive downloads
                    source_file = archive_file(local_path, message.file.name, message.file.size, message.file.mime_type, file_hash, msg_summary, archive_date=end_time, source_date=message.date)
                    source_files.append(source_file)

                    # Cleanup local file
                    os.remove(local_path)
            else: 
                msg_summary = create_message_summary(message, chat, chat_entity_id, from_entity, fwd_from_entity, file_hash=None)

        except Exception as e:  # catch anything that goes wrong during download
            print(f"Error downloading file: {e}")
            raise
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

        chat_history.append(msg_summary)

    return {
        "start_time": start_time,
        "end_time": end_time,
        "chat_title": chat.title,
        "chat_id": chat.id,
        "entity_id": chat_entity_id,
        "chat_type": type(chat).__name__,
        "chat_history": chat_history,
        "source_files": source_files
    }


def list_chat_archives(chat_type: str, chat_id: int, start_date: pendulum.DateTime, end_date: pendulum.DateTime, file_location: str = "telegram_chat_archives"):

    storage_location = os.getenv('STORAGE_LOCATION', 's3://tietl')
    base_path = f"{storage_location.rstrip('/')}/{file_location.rstrip('/')}"
    fs = get_fs()
    files = fs.ls(base_path)

    pattern = re.compile(
        rf"chat_archive_{chat_type}{chat_id}_(.+?)Z-(.+?)Z\.parquet$"
    )

    matching_files = []
    for f in files:
        fname = f.split("/")[-1]
        m = pattern.match(fname)
        if not m:
            continue

        file_start_str, file_end_str = m.groups()
        file_start = pendulum.parse(file_start_str + "Z")
        file_end = pendulum.parse(file_end_str + "Z")

        # Check for overlap between [file_start, file_end] and [start_date, end_date]
        if file_start <= end_date and file_end >= start_date:
            full_path = f"{base_path}/{fname}"
            matching_files.append(full_path)

    return matching_files



def list_file_sources(hash: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, file_location: str = "telegram_downloads"):

    storage_location = os.getenv('STORAGE_LOCATION', 's3://tietl')
    base_path = f"{storage_location.rstrip('/')}/{file_location.rstrip('/')}/{hash}"
    fs = get_fs()
    files = fs.ls(base_path)

    pattern = re.compile(
        rf"source_(.+?)Z\.json$"
    )

    matching_files = []
    for f in files:
        fname = f.split("/")[-1]
        m = pattern.match(fname)
        if not m:
            continue

        file_end_str = m.groups()
        file_end = pendulum.parse(file_end_str[0] + "Z")

        # Check if file_end is between start_date and end_date
        if file_end >= start_date and file_end <= end_date:
            full_path = f"{base_path}/{fname}"
            matching_files.append(full_path)

    return matching_files