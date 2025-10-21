import argparse
import fastavro
import pathlib
import pyiceberg.table
import time

import drive_stats_iceberg_table


def _parse_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()

    default_s3_endpoint: str = "https://s3.us-west-004.backblazeb2.com"
    default_b2_bucket_name: str = "drivestats-iceberg"
    default_b2_region: str = "us-west-004"
    default_table_path: str = "drivestats"

    arg_parser.add_argument("--s3-endpoint",
                            default=default_s3_endpoint,
                            help=f"S3 Endpoint (default: \"{default_s3_endpoint}\")")

    arg_parser.add_argument("--b2-region",
                            default=default_b2_region,
                            help=f"B2 Region (default: \"{default_b2_region}\")")

    arg_parser.add_argument("--bucket-name",
                            default=default_b2_bucket_name,
                            help=f"B2 Bucket Name (default: \"{default_b2_bucket_name}\")")

    arg_parser.add_argument("--table-path",
                            default=default_table_path,
                            help=f"B2 Bucket Table Path (default: \"{default_table_path}\")")

    arg_parser.add_argument("b2_access_key",
                            help="Backblaze B2 Access Key" )
    arg_parser.add_argument("b2_secret_access_key",
                            help="Backblaze B2 Secret Access Key" )
    arg_parser.add_argument("avro_output_file",
                            help="Path to output Avro file")

    return arg_parser.parse_args()


def _main() -> None:
    args: argparse.Namespace = _parse_args()

    # What's latest schema of the Backblaze drive stats?
    print("\nFetching current schema from Apache Iceberg table hosted in Backblaze B2...")
    operation_begin_time: float = time.perf_counter()
    current_schema_uri: str = drive_stats_iceberg_table.current_metadata_file_s3_uri(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path )
    operation_end_time: float = time.perf_counter()

    base_filename: str = pathlib.Path(current_schema_uri).name

    print(f"\tCurrent schema: {base_filename}")
    print(f"\t\tSchema URI: {current_schema_uri}")
    print(f"\tTime to retrieve schema from Backblaze B2: {operation_end_time - operation_begin_time:.03f} seconds")

    print("\nOpening Backblaze Drive Stats Apache Iceberg table hosted in Backblaze B2...")
    operation_begin_time: float = time.perf_counter()
    static_table: pyiceberg.table.StaticTable = drive_stats_iceberg_table.open_drive_stats_table(
        args.s3_endpoint,
        args.b2_region,
        args.b2_access_key,
        args.b2_secret_access_key,
        current_schema_uri
    )
    operation_end_time: float = time.perf_counter()
    print(f"\tStatic table opened at s3://{args.bucket_name}/{args.table_path}")
    print(f"\tTime to open table: {operation_end_time - operation_begin_time:.03f} seconds")

    # print("Schema:\n")
    # print(static_table.schema())

    backblaze_drive_stats_avro_schema = {
        'name': 'Backblaze Drive Stats Record',
        'type': 'record',
        'fields': [
            {
                'name': 'date',
                'type': 'string',
            },

            {
                'name': 'serial_number',
                'type': 'string',
            },

            {
                'name': 'model',
                'type': 'string',
            },

            {
                'name': 'capacity_bytes',
                'type': 'long',
            },

            {
                'name': 'failure',
                'type': 'boolean',
            },
        ]
    }
    parsed_schema = fastavro.parse_schema(backblaze_drive_stats_avro_schema)

    print("\nReading table incrementally with pyarrow batches and writing to Avro output file...")
    operation_begin_time: float = time.perf_counter()

    # Open in write mode to truncate if it exists, then immediately close
    with open(args.avro_output_file, "wb") as _:
        pass

    with open(args.avro_output_file, "a+b") as avro_handle:

        for record_batch in static_table.scan(
            selected_fields=(
                'date',
                'serial_number',
                'model',
                'capacity_bytes',
                'failure',
            )
        ).to_arrow_batch_reader():
            avro_queue = []
            for drive_entry in record_batch.to_pylist():
                avro_queue_entry = {
                    'date': drive_entry['date'].isoformat(),
                    'serial_number': drive_entry['serial_number'],
                    'model': drive_entry['model'],
                    'capacity_bytes': drive_entry['capacity_bytes'],
                }
                if drive_entry['failure'] == 1:
                    avro_queue_entry['failure'] = True
                else:
                    avro_queue_entry['failure'] = False

                avro_queue.append(avro_queue_entry)

            fastavro.writer(avro_handle, parsed_schema, avro_queue)

    operation_end_time: float = time.perf_counter()
    print(f"\tTime to read Iceberg table and write to Avro: {operation_end_time - operation_begin_time:.03f} seconds")


if __name__ == "__main__":
    _main()
