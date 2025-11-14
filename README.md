bitcoin-data-platform/
│
├── docker-compose.yaml           # Main orchestration file
├── README.md                      # Comprehensive documentation
│
├── data/                          # Data directory (not in git)
│   └── bitcoin_data.csv          # Kaggle dataset (download required)
│
├── producer/                      # Producer service
│   ├── Dockerfile                # Python 3.12 container
│   ├── requirements.txt          # kafka-python, pandas
│   └── producer.py               # Main producer script
│
├── spark/                         # Spark streaming service
│   ├── Dockerfile                # Python 3.12 + Java 17
│   ├── requirements.txt          # pyspark, minio
│   └── spark_streaming.py        # Streaming processor
│
└── duckdb-updater/               # DuckDB updater service
    ├── Dockerfile                # Python 3.12 container
    ├── requirements.txt          # duckdb, minio, pyarrow
    └── duckdb_updater.py         # File monitor & loader