```sh
RL-database-indexing
.
├── DIRECTORY.md
├── Dockerfile
├── README.md
├── benchmark_cli                               - python cli for the benchmark
│   ├── __init__.py
│   ├── cli.py
│   └── performance
│       ├── __init__.py
│       ├── constants.py
│       ├── generate_data.py
│       ├── results
│       │   ├── power_test.csv
│       │   └── throughput_test.csv
│       ├── run_benchmark.py
│       ├── run_performance_test.py
│       ├── run_power_test.py
│       ├── run_throughput_test.py
│       ├── stream
│       │   ├── AbstractStream.py
│       │   ├── QueryStream.py
│       │   ├── RefreshPair.py
│       │   ├── RefreshStream.py
│       │   ├── __init__.py
│       └── utils.py
├── dbgen_mysql_patch                           - files to replace in official tpch_tools/dbgen directory from TPC-H documentation
│   ├── dss.ddl
│   ├── dss.ri
│   ├── loaddata.sql
│   ├── makefile
│   ├── queries
│   │   ├── 1.sql
│   │   ├── 10.sql
│   │   ├── 11.sql
│   │   ├── 12.sql
│   │   ├── 13.sql
│   │   ├── 14.sql
│   │   ├── 15.sql
│   │   ├── 16.sql
│   │   ├── 17.sql
│   │   ├── 18.sql
│   │   ├── 19.sql
│   │   ├── 2.sql
│   │   ├── 20.sql
│   │   ├── 21.sql
│   │   ├── 22.sql
│   │   ├── 3.sql
│   │   ├── 4.sql
│   │   ├── 5.sql
│   │   ├── 6.sql
│   │   ├── 7.sql
│   │   ├── 8.sql
│   │   └── 9.sql
│   └── tpcd.h
├── docker-compose.yml
├── generators
│   ├── generate_db.sh
│   ├── generate_queries.sh
│   ├── generate_refresh_data.sh
│   └── run_queries.sh
├── requirements.txt
└── tpch_tools                                  - official TPC-H tools directory - download from http://tpc.org/tpc_documents_current_versions/current_specifications5.asp
    └── .gitkeep
```
