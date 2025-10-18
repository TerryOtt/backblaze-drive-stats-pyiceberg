# backblaze-drive-stats-pyiceberg

Reading Apache Iceberg tables and writing to local Parquet file

## Usage

```bash
$ python backblaze_drive_stats_iceberg_to_parquet.py <b2_access_key> <b2_secret_access_key> <parquet_output_file> 
```

## Notes

* 2025 Q2 data
  * Need an r8g.8xlarge with 256 GB of mem
  * Consumes 140 GB of memory with the selected columns as of 2025 Q2 data
* Output file size: 4.3 GB 
* Runtime: ~100 seconds
