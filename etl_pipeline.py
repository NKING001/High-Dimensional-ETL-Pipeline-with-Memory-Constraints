import os
import time
import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def update_mean_var(mean, var, count, data):
    n = len(data)
    if n == 0:
        return mean, var, count
    total = count + n
    new_mean = data.mean(axis=0).values
    new_var = data.var(axis=0, ddof=0).values
    delta = new_mean - mean
    mean = mean + delta * (n / total)
    var = ((count * var) + (n * new_var) + (delta ** 2 * count * n / total)) / total
    return mean, var, total

def make_chunk(rows, cols, idx, dtype):
    np.random.seed(idx)
    data = np.random.randn(rows, cols).astype(dtype)
    data += idx * 0.1
    names = [f"feature_{i}" for i in range(cols)]
    return pd.DataFrame(data, columns=names)

def validate(chunk, cols, dtypes):
    if chunk.shape[1] != cols:
        raise ValueError("Column count mismatch")
    if chunk.isnull().values.any():
        raise ValueError("Null values detected")
    for c, t in dtypes.items():
        if chunk[c].dtype != t:
            raise ValueError("Schema mismatch")

def mem_usage(df):
    size = df.memory_usage(deep=True).sum() / (1024 ** 2)
    print(f"Memory usage: {size:.2f} MB")

def run_pipeline(chunks, rows, cols, dtype, out_dir):
    stats_file = "stats_checkpoint.npz"
    progress_file = "progress_checkpoint.json"
    os.makedirs(out_dir, exist_ok=True)
    if os.path.exists(stats_file):
        data = np.load(stats_file)
        mean = data["means"]
        var = data["vars_"]
        count = int(data["count"])
    else:
        mean = np.zeros(cols, dtype=dtype)
        var = np.zeros(cols, dtype=dtype)
        count = 0
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)
        stats_done = progress.get("stats_complete", False)
        done = set(progress.get("processed_chunks", []))
    else:
        stats_done = False
        done = set()
    names = [f"feature_{i}" for i in range(cols)]
    schema = {n: dtype for n in names}
    if not stats_done:
        for i in range(chunks):
            data = make_chunk(rows, cols, i, dtype)
            validate(data, cols, schema)
            mem_usage(data)
            mean, var, count = update_mean_var(mean, var, count, data)
            del data 
            time.sleep(0.1)
        np.savez(stats_file, means=mean, vars_=var, count=count)
        with open(progress_file, "w") as f:
            json.dump({"stats_complete": True, "processed_chunks": []}, f)
    std = np.sqrt(var).astype(dtype)
    std[std == 0] = 1
    for i in range(chunks):
        if i in done:
            continue
        data = make_chunk(rows, cols, i, dtype)
        validate(data, cols, schema)
        mem_usage(data)
        norm = (data.values - mean) / std
        out = pd.DataFrame(norm, columns=names)
        tmp = os.path.join(out_dir, f"part-{i:05d}.parquet.tmp")
        final = os.path.join(out_dir, f"part-{i:05d}.parquet")
        table = pa.Table.from_pandas(out, preserve_index=False)
        pq.write_table(table, tmp, compression="zstd")
        os.rename(tmp, final)
        done.add(i)
        with open(progress_file, "w") as f:
            json.dump({"stats_complete": True, "processed_chunks": list(done)}, f)
        del data, norm, out, table
        time.sleep(0.1)
    if len(done) == chunks:
        if os.path.exists(stats_file):
            os.remove(stats_file)
        if os.path.exists(progress_file):
            os.remove(progress_file)
    print(f"Processed {chunks * rows:,} rows with {cols} columns")
    print(f"Output directory: {out_dir}")
    print(f"Mean preview: {mean[:5]}")
    print(f"Std preview: {std[:5]}")

if __name__ == "__main__":
    print("ETL started at", time.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        run_pipeline(
            chunks=5,
            rows=100000,
            cols=50,
            dtype="float32",
            out_dir="normalized_parquet"
        )
    except Exception as e:
        print("Pipeline failed:", e)
        raise
    print("ETL completed successfully")