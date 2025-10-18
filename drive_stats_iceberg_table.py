import argparse
import pyiceberg.table
import s3fs


def current_metadata_file_s3_uri( b2_access_key: str,
                           b2_secret_access_key: str,
                           s3_endpoint: str,
                           s3_bucket_name: str,
                           table_path: str) -> str:

    s3_handle: s3fs.S3FileSystem = s3fs.S3FileSystem(
        key=b2_access_key,
        secret=b2_secret_access_key,
        endpoint_url=s3_endpoint
    )

    bucket_path: str = f"{s3_bucket_name}/{table_path}/metadata"

    files = s3_handle.ls(bucket_path)

    metadata_json_files: list[str] = []

    for curr_file in files:
        if curr_file.endswith(".metadata.json"):
            metadata_json_files.append(curr_file)

    # Latest metadata is latest rev, so end of sorted list
    latest_metadata_json_file: str = f"s3://{sorted(metadata_json_files)[-1]}"

    return latest_metadata_json_file


def open_drive_stats_table(s3_endpoint: str,
                           b2_region: str,
                           b2_access_key: str,
                           b2_secret_access_key: str,
                           current_schema_uri: str ) -> pyiceberg.table.StaticTable:

    schema_properties: dict[str, str] = {
        "s3.endpoint"           : s3_endpoint,
        "s3.region"             : b2_region,
        "s3.access-key-id"      : b2_access_key,
        "s3.secret-access-key"  : b2_secret_access_key,
    }

    static_table: pyiceberg.table.StaticTable = pyiceberg.table.StaticTable.from_metadata(
        current_schema_uri, schema_properties)

    return static_table


def _parse_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()

    default_s3_endpoint: str = "https://s3.us-west-004.backblazeb2.com"
    default_b2_region: str = "us-west-004"
    default_b2_bucket_name: str = "drivestats-iceberg"
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

    return arg_parser.parse_args()


def _main():
    args: argparse.Namespace = _parse_args()

    current_schema_uri: str = current_metadata_file_s3_uri(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path )

    print(f"Latest metadata file: {current_schema_uri}")

    print("\nOpening Backblaze Drive Stats Apache Iceberg table hosted in Backblaze B2...")
    drive_stats_table = open_drive_stats_table(
        args.s3_endpoint,
        args.b2_region,
        args.b2_access_key,
        args.b2_secret_access_key,
        current_schema_uri
    )

    print(drive_stats_table.schema())




if __name__ == '__main__':
    _main()
