import json
import logging
import pyiceberg
import pyiceberg.catalog


def _load_catalog() -> pyiceberg.catalog.Catalog:

    # Use REST Catalog (exposed by Nessie)
    catalog_properties: dict[str, str] = {
        'type'                  : 'rest',
        'uri'                   : 'http://localhost:19120/iceberg',
        'warehouse'             : 's3://drivestats-iceberg',
        's3.access-key-id'      : '0045f0571db506a0000000017',
        's3.secret-access-key'  : 'K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE',
        's3.endpoint'           : 'https://s3.us-west-004.backblazeb2.com',
        's3.region'             : 'us-west-004',
    }
    
    catalog: pyiceberg.catalog.Catalog = pyiceberg.catalog.load_catalog("backblaze_b2", **catalog_properties)

    return catalog


def _main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    catalog: pyiceberg.catalog.Catalog = _load_catalog()

    print(json.dumps(catalog.properties, indent=2))

    print( f"Namespaces: {catalog.list_namespaces()}")

    # Try to open drivestats table
    table = catalog.load_table( "default.drivestats" )
    print( table.schema )
    
    # Check out StaticTable that reads a table straight from metadata, completely bypassing catalog.  You have my
    #   attention. Use s3 to walk the dir and get latest version of metadata, and we can skip these shenanigans

    # Looks like Backblaze writer script uses namespace of "default": https://github.com/backblaze-b2-samples/drivestats2iceberg




if __name__ == "__main__":
    _main()
