# High-Dimensional-ETL-Pipeline-with-Memory-Constraints
A scalable Python-based ETL pipeline that processes large datasets in chunks, computes running statistics, normalizes data, and writes fault-tolerant Parquet outputs with checkpointing.

Chunked ETL Pipeline with Parquet Output
Overview

This repository contains a scalable, memory-efficient ETL pipeline implemented in Python.
It is designed to process large datasets in fixed-size chunks, compute global mean and variance incrementally, normalize the data, and write the results to compressed Parquet files.

The pipeline supports fault tolerance using checkpointing, allowing safe recovery from interruptions without reprocessing completed chunks.

Key Features

Chunk-based data processing for large-scale datasets

Incremental mean and variance calculation (numerically stable)

Data validation and schema enforcement

Memory usage monitoring per chunk

Checkpointing for resumable execution

Normalized output stored in Parquet format

Compression using Zstandard (zstd)

Architecture

ETL Flow:

Generate or ingest data in chunks

Validate schema and data integrity

Compute global statistics incrementally

Normalize data using global mean and standard deviation

Write normalized chunks to Parquet files

Track progress using checkpoints

Technologies Used

Python 3

NumPy

Pandas

PyArrow

Parquet (zstd compression)

Project Structure
.
├── etl_pipeline.py
├── normalized_parquet/
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
├── stats_checkpoint.npz
├── progress_checkpoint.json
└── README.md

How It Works

Mean & Variance Update:
Uses an incremental algorithm to compute global statistics without loading all data into memory.

Checkpointing:
Saves progress to disk so the pipeline can resume after failure.

Normalization:
Each chunk is normalized using the global mean and standard deviation.

Atomic Writes:
Writes temporary Parquet files and renames them to ensure consistency.

How to Run
python etl_pipeline.py


You can modify parameters inside run_pipeline():

run_pipeline(
    chunks=5,
    rows=100000,
    cols=50,
    dtype="float32",
    out_dir="normalized_parquet"
)

Use Cases

Data preprocessing for Machine Learning pipelines

Large-scale ETL jobs

Feature normalization workflows

Data Engineering practice projects

Interview and internship portfolio projects

Future Improvements

Support for streaming input sources

Distributed execution (Dask / Spark)

Configurable input readers

Logging and metrics export

Unit and integration tests

Author

Nihar Kumar Patel
Computer Science Engineering Student
Interests: Data Engineering, Machine Learning, Motorsports Technology

License

This project is open-source and available under the MIT License.
