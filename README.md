# TIETL — Threat Intelligence ETL Framework

TIETL is an **open source Cyber Threat Intelligence ETL framework**
built on top of [Apache Airflow](https://airflow.apache.org/).\
It enables collection of cyber threat intelligence (CTI) from various
sources such as **Telegram chatrooms** and **combolists**, and provides
a flexible YAML-based configuration system for defining ETL pipelines with low or no code.

TIETL comes with with **Elasticsearch** for powerful querying and
search, and uses **Kibana** for data visualization, analysis, and
reporting.

![Tietl Architecture](https://github.com/IlyasMakari/tietl/blob/main/tietl-architecture.png?raw=true)

## Features

-   Collect CTI data from **Telegram**, combolists, and other online
    sources
-   Define ETL pipelines using simple **YAML files** and **YARA rules**
-   Store and query CTI in **Elasticsearch**
-   Analyze and visualize CTI with **Kibana dashboards**
-   Local testing setup with **Docker + Minio (S3 alternative)**
-   Automated and orchestrated with **Apache Airflow**


## Installation

### Requirements

-   [Docker](https://docs.docker.com/get-docker/)
-   [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Setup

``` bash
git clone https://github.com/IlyasMakari/tietl.git
```

1.  Fill in your **Telegram API key** in the `.env` file
2.  Configure your **Elasticsearch** connection in the `.env` file
3.  Configure your **S3/Minio endpoint** in the `.env` file

For **local testing**, TIETL comes with a Docker setup under `/servers`
that launches local servers:
- Minio (S3 alternative)
- Elasticsearch
- Kibana


### Telegram Setup

To scrape data from Telegram, add a **Telethon session** named
`tietl.session` inside the `telethon_sessions/` folder.


## Running TIETL

Start the project with:

``` bash
astro dev start
```

Once running, access the **Airflow UI** at:
👉 <http://localhost:8080>

From here, you can monitor, trigger, and manage your ETL pipelines.

## Creating ETL Pipelines

TIETL allows you to define ETL pipelines using YAML files. This makes it easy to scrape data from Telegram chatrooms, download files, and apply custom rules to determine which files should be downloaded and analyzed.

### Example: `dags/telegram_chats/alien_cloud.yaml`

```yaml
txt_alien:
  default_args:
    retries: 0
  start_date: "2023-01-01"
  schedule_interval: "@daily"
  catchup: false
  tags: ["telegram", "infostealer", "chatrooms"]
  chat_info:
    chat_name: "alien"
    chat_entity: "t.me/+VwOmy2CzcjsxMjBh"
    chat_type: "Channel"
    chat_id: "2429498260"
  enable_downloads: True
  download_rules:
    allowed_mime_types: ["text/plain"]
  download_handlers:
    - yara: "alientxt_ulp.yar"
      run: "tools/ulp_parser/ulp_parser_cli.py --input \"$file_path\" --output-folder \"$output_path\" --file-hash \"$file_hash\" --max-fail-percent 5.0 --chunk-size 200000"
```

This example shows how to:  
- Define the **chat** to scrape from (`chat_info`)  
- Schedule a daily ETL run (`schedule_interval`)  
- Enable downloading of files (`enable_downloads`)  
- Apply **download rules** to filter files by MIME type
- Apply **file hander** to parse files based on YARA rules

You can create similar YAML files for other Telegram channels to extend your CTI pipelines.


## 📜 License

This project is licensed under the **AGPLv3**.
