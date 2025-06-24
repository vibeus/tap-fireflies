
import singer
from singer import Transformer, metadata


from tap_fireflies.client import FirefliesClient
from tap_fireflies.streams import STREAMS

LOGGER = singer.get_logger()

def translate_state(state):
    """
    Tap was used to write bookmark using custom get_bookmark and write_bookmark methods,
    in which case the state looked like the following format.
    {
        "bookmarks": {
            "users": "2021-12-20T21:30:35.000000Z"
        }
    }

    The tap now uses get_bookmark and write_bookmark methods of singer library,
    which expects state in a new format with replication keys.
    So this function should be called at beginning of each run to ensure the state is translated to a new format.
    {
        "bookmarks": {
            "users": {
                "updated_at": "2021-12-20T21:30:35.000000Z"
            }
        }
    }
    """

    # Iterate over all streams available in state
    for stream, bookmark in state.get("bookmarks", {}).items():
        # If bookmark is directly present without replication_key(old format)
        # then add replication key at inner level
        if isinstance(bookmark, str):
            replication_key = STREAMS[stream].replication_key
            state["bookmarks"][stream] = {replication_key : bookmark}

    return state

def get_streams_to_sync(catalog, selected_streams, selected_stream_names):
    """
        Get streams to sync
    """
    streams_to_sync = []

    for stream in selected_streams:
        streams_to_sync.append(stream)
        stream_obj = STREAMS.get(stream.tap_stream_id)

    return streams_to_sync

def sync(config, state, catalog):
    """ Sync data from tap source """

    access_token = config.get('access_token')
    client = FirefliesClient(access_token, config.get('request_timeout')) # pass request_timeout parameter from config

    # Translate state to the new format with replication key in the state
    state = translate_state(state)

    selected_stream_names = []
    selected_streams = list(catalog.get_selected_streams(state))
    for stream in selected_streams:
        selected_stream_names.append(stream.tap_stream_id)

    with Transformer() as transformer:
        for stream in get_streams_to_sync(catalog, selected_streams, selected_stream_names):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, catalog, selected_stream_names)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Starting sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(state, stream_schema, stream_metadata, config, transformer)
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)