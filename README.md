# backblaze-drive-stats-pyiceberg

Reading Apache Iceberg tables and writing to local Parquet and Avro files.

## Usage

```bash
$ python backblaze_drive_stats_iceberg_to_parquet.py <b2_access_key> <b2_secret_access_key> <parquet_output_file> 
$ python backblaze_drive_stats_iceberg_to_csv.py    <b2_access_key> <b2_secret_access_key> <avro_output_file>
```

## Notes

* 2025 Q23data
  * EC2 Instance: c8g.xlarge with 8 GB of mem
  * Output file size
    * Parquet: 4.3 GB (~100 seconds) 
    * Avro: ~100 seconds
