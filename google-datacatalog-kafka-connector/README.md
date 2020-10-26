# google-datacatalog-kafka-connector

Common resources for Data Catalog RDBMS connectors.

TBA: widgets

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- TOC -->

   * [google-datacatalog-kafka-connector](#google-datacatalog-kafka-connector)
      * [Table of Contents](#table-of-contents)
      * [1. Installation](#1-installation)
         * [1.1. Mac/Linux](#11-maclinux)
         * [1.2. Windows](#12-windows)
         * [1.3. Install from source](#13-install-from-source)
            * [1.3.1. Get the code from the repository](#131-get-the-code-from-the-repository)
            * [1.3.2. Create and activate a <em>virtualenv</em>](#132-create-and-activate-a-virtualenv)
            * [1.3.3. Install the library](#133-install-the-library)
      * [2. Environment setup](#2-environment-setup)
         * [2.1. Auth credentials](#21-auth-credentials)
            * [2.1.1. Create a service account and grant it below roles](#211-create-a-service-account-and-grant-it-below-roles)
            * [2.1.2. Download a JSON key and save it as](#212-download-a-json-key-and-save-it-as)
         * [2.2. Set environment variables](#22-set-environment-variables)
      * [3. Run entry point](#3-run-entry-point)
         * [3.1. Run Python entry point](#31-run-python-entry-point)
      * [4. Scripts inside tools](#4-scripts-inside-tools)
         * [4.1. Run clean up](#41-run-clean-up)
         * [4.2 Generate random metadata for performance testing](#42-generate-random-metadata-for-performance-testing)
      * [5. Developer environment](#5-developer-environment)
         * [5.1. Install and run YAPF formatter](#51-install-and-run-yapf-formatter)
         * [5.2. Install and run Flake8 linter](#52-install-and-run-flake8-linter)
         * [5.3. Install the package in editable mode (i.e. setuptools “develop mode”)](#53-install-the-package-in-editable-mode-ie-setuptools-develop-mode)
         * [5.4. Run the unit tests](#54-run-the-unit-tests)
      * [6. Metrics](#6-metrics)
      * [7. Troubleshooting](#7-troubleshooting)

<!-- tocstop -->

-----

## 1. Installation

Install this library in a [virtualenv][1] using pip. [virtualenv][1] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][1], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.6 or Python 3.7.


### 1.1. Mac/Linux

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-kafka-connector
```


### 1.2. Windows

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-kafka-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code from the repository

````bash
TBA
````

#### 1.3.2. Create and activate a *virtualenv*

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
```

#### 1.3.3. Install the library

```bash
pip install .
```

## 2. Environment setup

### 2.1. Auth credentials

#### 2.1.1. Create a service account and grant it below roles

- Data Catalog Admin

#### 2.1.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/postgresql2dc-credentials.json`

> Please notice this folder and file will be required in next steps.

### 2.2. Set environment variables

Replace below values according to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=data_catalog_credentials_file

export KAFKA2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export KAFKA2DC_DATACATALOG_LOCATION_ID=google_cloud_location_id
export KAFKA2DC_KAFKA_HOST=kafka_bootstrap_server

```

Export below variables to enable optional ingestion of the schemas from the Schema Registry. 
The url is required, the rest depends on your Schema Registry authentication setup.

```bash
# Required to connect to the Schema Registry

export KAFKA2DC_SCHEMA_REGISTRY_URL=url/to/schema/registry

# All the following are optional and depend on your Schema Registry authentication setup

export KAFKA2DC_SCHEMA_REGISTRY_SSL_CA_LOCATION=path_to_CA_certificate_file
export KAFKA2DC_SCHEMA_REGISTRY_SSL_CERT_LOCATION=path_to_public_key
export KAFKA2DC_SCHEMA_REGISTRY_SSL_KEY_LOCATION=path_to_private_key

# Client HTTP credentials in the form of username:password for the Schema Registry

export KAFKA2DC_SCHEMA_REGISTRY_AUTH_USER_INFO=username:password
```

## 3. Run entry point

### 3.1. Run Python entry point

Arguments related to the Schema Registry are optional and should be provided only if you want the schema data to be
ingested in the Data Catalog. In this case, --schema-registry-url is required, the rest depends on your 
authentication setup.

- Virtualenv

```bash
google-datacatalog-kafka-connector \
--datacatalog-project-id=$KAFKA2DC_DATACATALOG_PROJECT_ID \
--datacatalog-location-id=$KAFKA2DC_DATACATALOG_LOCATION_ID \
--kafka-host=$KAFKA2DC_KAFKA_HOST \
--schema-registry-url=$KAFKA2DC_SCHEMA_REGISTRY_URL \
--schema-registry-ssl-ca-location=$KAFKA2DC_SCHEMA_REGISTRY_SSL_CA_LOCATION \
--schema-registry-ssl-cert-location=$KAFKA2DC_SCHEMA_REGISTRY_SSL_CERT_LOCATION \
--schema-registry-ssl-key-location=$KAFKA2DC_SCHEMA_REGISTRY_SSL_KEY_LOCATION \
--schema-registry-auth-user-info=$KAFKA2DC_SCHEMA_REGISTRY_AUTH_USER_INFO \
--service-account-path=$GOOGLE_APPLICATION_CREDENTIALS

```

## 4. Scripts inside tools

### 4.1. Run clean up

```bash
# List of projects split by comma. Can be a single value without comma
export KAFKA2DC_DATACATALOG_PROJECT_IDS=my-project-1,my-project-2
```

```bash
# Run the clean up
python tools/cleanup_datacatalog.py --datacatalog-project-ids=$KAFKA2DC_DATACATALOG_PROJECT_IDS

```

### 4.2 Generate random metadata for performance testing

You can use tools/metadata-generator.py to generate random topics on a Kafka cluster
```bash
# List of bootstrap.servers
export KAFKA2DC_KAFKA_HOST=kafka_bootstrap_server
```

The only required argument is --kafka-host, the rest is optional

```bash
# Run the metadata generator
python tools/metadata_generator.py --kafka-host=$KAFKA2DC_KAFKA_HOST \
--number-topics <number of topics to generate, 1000 by default> \
--max-replication-factor <number> --max-partitions <number> 
```


## 5. Developer environment

### 5.1. Install and run YAPF formatter

```bash
pip install --upgrade yapf

# Auto update files
yapf --in-place --recursive src tests

# Show diff
yapf --diff --recursive src tests

# Set up pre-commit hook
# From the root of your git project.
curl -o pre-commit.sh https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh
chmod a+x pre-commit.sh
mv pre-commit.sh .git/hooks/pre-commit
```

### 5.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 5.3. Install the package in editable mode (i.e. setuptools “develop mode”)

```bash
pip install --editable .
```

### 5.4. Run the unit tests

```bash
python setup.py test
```

## 6. Metrics

This execution was collected from a Kafka 2.6.0 cluster with 3 brokers populated with 1000 topics, running the kafka2datacatalog connector to ingest
those entities into Data Catalog. This shows what the user might expect when running this connector.

The following metrics are not a guarantee, they are approximations that may change depending on the environment, network and execution.


| Metric                     | Description                                       | VALUE            |
| ---                        | ---                                               | ---              |
| **elapsed_time**           | Elapsed time from the execution.                  | 11 Minutes       |
| **entries_length**         | Number of entities ingested into Data Catalog.    | 1001             |


## 7. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will be raised and logged with the following detailement, depending on the performed operation READ/WRITE/SEARCH: 
```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```
For more info about Data Catalog quota, go to: [Data Catalog quota docs](https://cloud.google.com/data-catalog/docs/resources/quotas).

[1]: https://virtualenv.pypa.io/en/latest/
