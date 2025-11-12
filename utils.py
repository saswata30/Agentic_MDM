# Save Parquet while avoiding Delta TIMESTAMP_NTZ feature unless explicitly enabled.
# Default behavior: convert datetime columns to ISO8601 UTC strings with millisecond precision ("...Z"),
# which prevents Spark/Delta from inferring TIMESTAMP_NTZ and requiring the timestampNtz table feature.
# Opt-in to real Parquet timestamps by setting environment variable USE_PARQUET_TIMESTAMPS=1.

import os
import shutil
from typing import List

import pandas as pd


def _datetime_cols(df: pd.DataFrame) -> List[str]:
  """Return names of tz-aware and tz-naive datetime columns."""
  cols = list(df.select_dtypes(include=['datetimetz', 'datetime64[ns]']).columns)
  # pandas can sometimes hide tz-aware dtypes; be defensive:
  # Any object/series that looks like datetime will be left alone to avoid false positives.
  return cols


def _to_iso_utc_ms_inplace(df: pd.DataFrame) -> None:
  """Convert datetime columns to ISO8601 UTC strings with millisecond precision, ending with 'Z'.
  This avoids creating Parquet logical TIMESTAMP columns (and thus TIMESTAMP_NTZ in Spark).
  """
  for c in _datetime_cols(df):
    # Normalize to UTC, floor to ms
    s = pd.to_datetime(df[c], utc=True, errors='coerce').dt.floor('ms')
    # Format to ISO with Z suffix; NaT stays 'NaT'
    # NOTE: use .dt.strftime with %f then trim to ms
    # Example: '2025-06-22T13:45:12.123Z'
    df[c] = s.dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str.slice(0, 23).astype(str) + 'Z'
    df.loc[s.isna(), c] = 'NaT'  # keep explicit NaT string for missing


def _normalize_dt_to_parquet_ts_inplace(df: pd.DataFrame) -> None:
  """Original behavior: keep datetime columns as real Parquet timestamps (ms).
  WARNING: In Spark/Delta this maps to TIMESTAMP_NTZ and may require the 'timestampNtz' table feature.
  """
  # tz-aware -> UTC -> floor to ms -> drop tz -> datetime64[ms]
  for c in df.select_dtypes(include=['datetimetz']).columns:
    s = pd.to_datetime(df[c], utc=True, errors='coerce').dt.floor('ms')
    df[c] = s.dt.tz_localize(None).astype('datetime64[ms]')

  # naive datetime64[ns] -> interpret as UTC -> floor to ms -> datetime64[ms]
  for c in df.select_dtypes(include=['datetime64[ns]']).columns:
    s = pd.to_datetime(df[c], utc=True, errors='coerce').dt.floor('ms')
    df[c] = s.dt.tz_localize(None).astype('datetime64[ms]')


def _choose_engine_kwargs():
  """Pick Parquet engine and args."""
  try:
    import pyarrow  # noqa: F401

    return 'pyarrow', {
      'coerce_timestamps': 'ms',
      'allow_truncated_timestamps': True,
      'use_deprecated_int96_timestamps': False,
    }
  except Exception:
    pass
  try:
    import fastparquet  # noqa: F401

    return 'fastparquet', {'times': 'ms'}
  except Exception:
    pass
  raise RuntimeError("Install 'pyarrow' (recommended) or 'fastparquet'.")


def save_to_parquet(df: pd.DataFrame, table_name: str, num_files: int = 5) -> None:
  """Write Parquet ensuring:
    - By default, datetime columns are serialized as ISO8601 UTC strings (ms, 'Z') to avoid TIMESTAMP_NTZ.
    - If USE_PARQUET_TIMESTAMPS=1, datetimes are written as real Parquet timestamps (TIMESTAMP_MILLIS).
    - Integer epoch columns remain integers.
    - Multiple part files if desired.
    - If CATALOG, SCHEMA, and VOLUME env vars are set, writes directly to /Volumes/{catalog}/{schema}/{volume}/
  Mutates `df` in place.
  """
  use_parquet_ts = os.getenv('USE_PARQUET_TIMESTAMPS', '0') == '1'

  if use_parquet_ts:
    _normalize_dt_to_parquet_ts_inplace(df)
  else:
    _to_iso_utc_ms_inplace(df)

  # Ensure table_name has raw_ prefix (avoid double prefix)
  if not table_name.startswith('raw_'):
    table_name = f'raw_{table_name}'

  # Check if we should write to Databricks Volumes or local filesystem
  catalog = os.getenv('CATALOG')
  schema = os.getenv('SCHEMA')
  volume = os.getenv('VOLUME')

  if catalog and schema and volume:
    # Ensure catalog exists (DAB doesn't support catalog creation, only schemas and volumes)
    try:
      from pyspark.sql import SparkSession
      spark = SparkSession.builder.getOrCreate()

      # Check and create catalog if needed
      catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
      if catalog not in catalogs:
        print(f"Creating catalog: {catalog}")
        spark.sql(f"CREATE CATALOG {catalog}")
      else:
        print(f"Catalog {catalog} already exists ✅")

    except Exception as e:
      print(f"Warning: Could not ensure catalog exists: {e}")
      print("Proceeding with write attempt...")

    # Write directly to Databricks Volumes
    # Note: Schema and volume are created by DAB before this task runs
    outdir = f'/Volumes/{catalog}/{schema}/{volume}/{table_name}'
  else:
    # Write to local filesystem
    outdir = f'data/{table_name}'

  # Create directory (works for both Volumes and local filesystem)
  os.makedirs(outdir, exist_ok=True)

  engine, engine_kwargs = _choose_engine_kwargs()

  n = len(df)
  location = f'{catalog}.{schema}.{volume}' if (catalog and schema and volume) else 'local filesystem'

  if n < num_files:
    df.to_parquet(f'{outdir}/part_000.parquet', index=False, engine=engine, **engine_kwargs)
    msg = 'TIMESTAMP_MILLIS' if use_parquet_ts else 'ISO8601 strings'
    print(f'✓ Saved {n:,} rows to {outdir}/ (1 file) [{location}] — datetimes as {msg}')
  else:
    chunk = (n + num_files - 1) // num_files
    part = 0
    for start in range(0, n, chunk):
      end = min(start + chunk, n)
      if start >= end:
        break
      df.iloc[start:end].to_parquet(
        f'{outdir}/part_{part:03d}.parquet', index=False, engine=engine, **engine_kwargs
      )
      part += 1
    msg = 'TIMESTAMP_MILLIS' if use_parquet_ts else 'ISO8601 strings'
    print(f'✓ Saved {n:,} rows to {outdir}/ ({part} files) [{location}] — datetimes as {msg}')

  # If writing to Databricks Volumes, create Delta table from parquet files
  if catalog and schema and volume:
    try:
      from pyspark.sql import SparkSession
      spark = SparkSession.builder.getOrCreate()

      # Create Delta table using read_files
      create_table_sql = f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name}
        COMMENT 'Raw data table generated from parquet files'
        AS SELECT *
        FROM read_files('{outdir}', format => 'parquet', pathGlobFilter => '*.parquet')
      """

      print(f"Creating Delta table: {catalog}.{schema}.{table_name}")
      spark.sql(create_table_sql)
      print(f"✓ Delta table {catalog}.{schema}.{table_name} created successfully")

    except Exception as e:
      print(f"Warning: Could not create Delta table: {e}")
      print("Parquet files are still available in the volume.")
