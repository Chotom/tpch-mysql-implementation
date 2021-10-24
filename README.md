# tpch-mysql-implementation
Project's tree with short descriptions: [DIRECTORY.md](./DIRECTORY.md)

#### Table of contents: 
1. [Description](#description)
1. [Setup](#setup)
1. [Credits](#credits)

## Description
Python implementation for TPC-H benchmark of MYSQL database. 
CLI tools for measures database performance in docker.

## Setup

Download and extract tpch tools (dbgen and qgen) to tpch_tools dir (required to run project):
[download link](http://tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.0&mode=CURRENT-ONLY "tpch tools")

Build and prepare docker images:
```shell
docker-compose build
```

Run containers:
```shell
docker-compose up
```

Enter container:
```shell
docker exec -it rl-database-indexing_benchmark_tpch_1 /bin/bash
```

Generate database to test (in benchmark container):
```shell
python3 benchmark_cli/cli.py generate_database
```

Define values in config file:
```yaml
refresh_file_index: 1
start_seed: 1
stream_count: 2
```

Run performance test (in benchmark container):
```shell
python3 benchmark_cli/cli.py --help
python3 benchmark_cli/cli.py run_benchmark
```



## Credits
Tomasz Czochański and Michał Matczak