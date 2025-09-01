#!/usr/bin/env python3
import argparse
import hashlib
import fsspec
import os
import uuid
import pandas as pd
import dask.dataframe as dd
import numpy as np
import re
import tldextract
from urllib.parse import urlparse
from include.filesystem import get_fs

from ulp_parser import parse_ulp_file

CHUNK_SIZE = 500_000

def cleanup_dataframe(df: dd.DataFrame) -> dd.DataFrame:
    passwords_to_delete = ["Decryption failed.", "[NOT_SAVED]", "Old or unknown version."]
    df["password"] = df["password"].replace(passwords_to_delete, np.nan)

    email_pattern = r'^([a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,})$'
    row_with_email_host = (
        (df['status'] == 'success') &
        (~df['host'].isna()) &
        (df['host'] != '') &
        df['host'].str.match(email_pattern, na=False)
    ).persist()

    df['host'] = df['host'].mask(row_with_email_host, '')
    df['login'] = df['login'].mask(row_with_email_host, '')
    df['password'] = df['password'].mask(row_with_email_host, '')

    df = df.drop(columns=['status'], errors='ignore')

    return df

def get_host_scheme(url):
    try:
        if url:
            tldextracted = tldextract.extract(url)
            parsed = urlparse(url)
            if parsed.scheme:
                parts = [tldextracted.subdomain, tldextracted.domain, tldextracted.suffix]
                delete = ".".join(filter(None, parts))
                scheme = parsed.scheme.replace(delete, "")
                if '.' not in scheme:
                    return scheme
    except:
        return None

def get_host_domain(url):
    try:
        if url:
            tldextracted = tldextract.extract(url)
            return ".".join(filter(None, [tldextracted.domain, tldextracted.suffix]))
    except:
        return None

def get_host_subdomain(url):
    try:
        if url:
            tldextracted = tldextract.extract(url)
            return ".".join(filter(None, [tldextracted.subdomain, tldextracted.domain, tldextracted.suffix]))
    except:
        return None

def get_host_tld(url):
    try:
        if url:
            tldextracted = tldextract.extract(url)
            return tldextracted.suffix
    except:
        return None

def extract_port(url: str):
    try:
        if url:
            parsed = urlparse(url)
            return parsed.port
    except:
        return None

def extract_email_domain(email: str):
    try:
        if email:
            EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
            if EMAIL_REGEX.match(email):
                return email.split('@')[1].lower()
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description="Parse ULP log files into Parquet format")
    parser.add_argument("--input", required=True, help="Path to input file (local or s3://)")
    parser.add_argument("--output-folder", required=True, help="Folder to write parquet files (local or s3://)")
    parser.add_argument("--max-fail-percent", type=float, default=2.0, help="Maximum allowed parsing failure percent")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Number of lines per parquet file chunk")
    parser.add_argument("--file-hash", required=True, help="File hash associated with the ULP log file")
    args = parser.parse_args()

    input_path = args.input
    output_folder = args.output_folder
    output_folder = os.path.join(output_folder, "combolist_credentials")
    max_fail_pct = args.max_fail_percent
    chunk_size = args.chunk_size
    file_hash = args.file_hash

    # Determine filesystem
    input_fs = get_fs() if input_path.startswith("s3://") else fsspec.filesystem("file")
    output_fs = get_fs() if output_folder.startswith("s3://") else fsspec.filesystem("file")

    # Remove previous output folder if exists
    if output_fs.exists(output_folder):
        output_fs.rm(output_folder, recursive=True)
    output_fs.mkdir(output_folder)

    # Create a tmp folder for intermediate results
    tmp_folder = f"/tmp/ulp_parse_{uuid.uuid4().hex}"
    os.makedirs(tmp_folder, exist_ok=True)

    buffer = []
    chunk_idx = 0

    total_lines = 0
    failed_lines = 0

    def flush_buffer():
        nonlocal chunk_idx, buffer
        if buffer:
            df_chunk = pd.DataFrame(
                buffer, columns=["line_no", "host", "login", "password", "raw_text", "status"]
            )
            chunk_file = f"{tmp_folder}/combolist_credentials.{chunk_idx}.parquet"
            with output_fs.open(chunk_file, "wb") as f:
                df_chunk.to_parquet(f, engine="pyarrow", index=False)
            buffer.clear()
            chunk_idx += 1

    def success_cb(lineno, line, data):
        nonlocal total_lines
        total_lines += 1
        buffer.append([lineno, data['host'], data['login'], data['password'], line, "success"])
        if len(buffer) >= chunk_size:
            print(f"Flushing chunk at line {lineno}")
            flush_buffer()

    def error_cb(status_log, lineno, line):
        nonlocal total_lines, failed_lines
        total_lines += 1
        failed_lines += 1
        buffer.append([lineno, None, None, None, line, str(status_log)])
        if len(buffer) >= chunk_size:
            print(f"Flushing chunk at line {lineno}")
            flush_buffer()

    try: 
        # Parse the file
        parse_ulp_file(
            path=input_path,
            possible_delimiters=(":", "|"),
            on_success=success_cb,
            on_error=error_cb,
            fs=input_fs
        )

        # Flush remaining lines
        flush_buffer()
        print(f"Parsing complete. Tmp output folder: {tmp_folder}")

        # Read parquet files into dataframe
        df = dd.read_parquet(tmp_folder, engine="pyarrow", filesystem=fsspec.filesystem("file"))

        # Verify failed lines percentage
        fail_pct = failed_lines / total_lines * 100
        print(f"Failed to parse {failed_lines} ({fail_pct:.2f}%) of the {total_lines} rows")
        if fail_pct > max_fail_pct:
            print(f"Parsing failure {fail_pct:.2f}% exceeds allowed {max_fail_pct}% - aborting.")
            exit(1)

        # Clean dataframe
        df = cleanup_dataframe(df)

        # Transformations
        df["host_scheme"] = df["host"].map(get_host_scheme, meta=('host_scheme', 'object'))
        android_mask = (df["host_scheme"] != "android").persist()
        df["host_domain"] = df["host"].map(get_host_domain, meta=('host_domain', 'object'))
        df["host_domain"] = df["host_domain"].where(android_mask, None)
        df["host_subdomain"] = df["host"].map(get_host_subdomain, meta=('host_subdomain', 'string'))
        df["host_subdomain"] = df["host_subdomain"].where(android_mask, None)
        df["host_tld"] = df["host"].map(get_host_tld, meta=('host_tld', 'string'))
        df["host_tld"] = df["host_tld"].where(android_mask, None)
        df["host_port"] = df["host"].map(extract_port, meta=('host_port', 'float64'))
        df["host_port"] = df["host_port"].where(android_mask, None)
        df["host_port"] = df["host_port"].astype("Int64")
        df["login_email_subdomain"] = df["login"].map(extract_email_domain, meta=('login_email_subdomain', 'string'))
        df["login_email_domain"] = df["login_email_subdomain"].map(get_host_domain, meta=('login_email_domain', 'string'))
        df["login_email_tld"] = df["login_email_domain"].map(get_host_tld, meta=('login_email_tld', 'string'))
        df["file_hash"] = file_hash

        # Add unique id for the database
        df["_id"] = df.apply(lambda row: hashlib.sha256(f"{row['raw_text']}_{row['file_hash']}".encode()).hexdigest(), axis=1, meta=('_id', 'string'))

        # Write final output
        df.to_parquet(output_folder, engine="pyarrow", write_index=False, filesystem=output_fs)
        print(f"Cleaned data saved to {output_folder}")

    except Exception as e:
        print(f"Error occurred during parsing: {e}")
        output_fs.rm(tmp_folder, recursive=True)
        exit(1)
    finally:
        output_fs.rm(tmp_folder, recursive=True)

    




if __name__ == "__main__":
    main()
