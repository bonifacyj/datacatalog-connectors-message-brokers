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

- [google-datacatalog-kafka-connector](#google-datacatalog-kafka-connector)
  - [Table of Contents](#table-of-contents)
  - [1. Installation](#1-installation)
    - [1.1. Mac/Linux](#11-maclinux)
    - [1.2. Windows](#12-windows)
  - [2. Install from source](#2-install-from-source)
    - [2.1. Get the code](#21-get-the-code)
    - [2.2. Virtualenv](#22-virtualenv)
  - [3. Developer environment](#3-developer-environment)
    - [3.1. Install and run YAPF formatter](#31-install-and-run-yapf-formatter)
    - [3.2. Install and run Flake8 linter](#32-install-and-run-flake8-linter)
    - [3.3. Install the package in editable mode (i.e. setuptools “develop mode”)](#33-install-the-package-in-editable-mode-ie-setuptools-develop-mode)
    - [3.4. Run the unit tests](#34-run-the-unit-tests)

<!-- tocstop -->

-----

## 1. Installation

Install this library in a [virtualenv][1] using pip. [virtualenv][1] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][1], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.6+.


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

## 2. Install from source

### 2.1. Get the code from the repository

````bash
TBA
````

### 2.2. Virtualenv

Using *virtualenv* is optional, but strongly recommended.

##### 2.2.1. Install Python 3.6

##### 2.2.2. Create and activate a *virtualenv*

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
```

##### 2.2.3. Install

```bash
pip install .
```

## 3. Developer environment

### 3.1. Install and run YAPF formatter

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

### 3.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 3.3. Install the package in editable mode (i.e. setuptools “develop mode”)

```bash
pip install --editable .
```

### 3.4. Run the unit tests

```bash
python setup.py test
```

[1]: https://virtualenv.pypa.io/en/latest/
