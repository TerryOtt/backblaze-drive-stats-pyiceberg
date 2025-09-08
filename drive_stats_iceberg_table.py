import argparse
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


def _parse_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()

    default_s3_endpoint: str = "https://s3.us-west-004.backblazeb2.com"
    default_b2_bucket_name: str = "drivestats-iceberg"
    default_table_path: str = "drivestats"

    arg_parser.add_argument("--s3-endpoint",
                            default=default_s3_endpoint,
                            help=f"S3 Endpoint (default: \"{default_s3_endpoint}\")")

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

    # Let's see if we can find some metadata
    latest_metadata_json_file: str = current_metadata_file_s3_uri(
        args.b2_access_key,
        args.b2_secret_access_key,
        args.s3_endpoint,
        args.bucket_name,
        args.table_path )

    print(f"Latest metadata file: {latest_metadata_json_file}")


if __name__ == '__main__':
    _main()
