#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_fireflies.streams import create_stream
from tap_fireflies.custom_utils import Context

REQUIRED_CONFIG_KEYS = ["access_token", "endpoint_url", "start_date"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream = create_stream(stream_id)

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=stream.key_properties,
                metadata=stream.get_metadata(schema.to_dict()),
                replication_key=stream.replication_key,
                replication_method=stream.replication_method,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        data_stream = create_stream(stream.tap_stream_id)
        data_stream_state = state.get(stream.tap_stream_id, {})

        t = Transformer()
        for row in data_stream.get_tap_data(config, data_stream_state):
            schema = stream.schema.to_dict()
            mdata = metadata.to_map(stream.metadata)
            record = t.transform(row, schema, mdata)
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [record])
        Context.state['bookmarks'].update({stream.tap_stream_id: data_stream.state})
        singer.write_state(Context.state)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
