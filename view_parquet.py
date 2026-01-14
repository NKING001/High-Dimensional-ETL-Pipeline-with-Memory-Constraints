import os
import glob
import pandas as pd
parquet_folder = "normalized_parquet"
parquet_files = glob.glob(os.path.join(parquet_folder, "*.parquet"))
if not parquet_files:
    print("No parquet files found")
    exit()
all_data = pd.concat(
    [pd.read_parquet(file) for file in parquet_files],
    ignore_index=True
)
all_data.to_csv("normalized_all.csv", index=False)
sample_files = parquet_files[:2]
sample_data = pd.concat(
    [pd.read_parquet(file) for file in sample_files],
    ignore_index=True
)
print("Overall mean per column (should be close to 0)")
print(sample_data.mean().round(4))
print("Overall std per column (should be close to 1)")
print(sample_data.std().round(4))
print(f"Found {len(parquet_files)} files")
for file in sorted(parquet_files):
    try:
        df = pd.read_parquet(file)
        print(f"File: {os.path.basename(file)}")
        print(f"Rows: {df.shape[0]} Columns: {df.shape[1]}")
        print(df.head())
        print(df.dtypes)
        print("-" * 60)
    except Exception as error:
        print(f"Failed to read {file}: {error}")
