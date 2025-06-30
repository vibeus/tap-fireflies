"""
This module defines the stream classes and their individual sync logic.
Credit: https://github.com/singer-io/tap-intercom/blob/master/tap_intercom/streams.py
"""

import datetime
import hashlib
from typing import Iterator

import singer
from singer import Transformer, metrics, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.transform import transform, unix_milliseconds_to_datetime

from tap_fireflies.client import (FirefliesClient, FirefliesError)

LOGGER = singer.get_logger()

MAX_PAGE_SIZE = 50
FIREFLIES_MAX_NUM_OF_RECORDS = 50

class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used to extract records from external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    to_replicate = True
    path = None
    # endpoint is used for logging.
    endpoint = None
    params = {}
    data_key = 'data'
    schema_key = None

    def __init__(self, client: FirefliesClient, catalog, selected_streams):
        self.client = client
        self.catalog = catalog
        self.selected_streams = selected_streams

    def get_records(self, bookmark_datetime: datetime = None, stream_metadata=None) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :param stream_metadata: Stream metadata dict, if required by the child get_records()
            method.
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def generate_record_hash(self, original_record, fields_to_hash):
        """
            Function to generate the hash of selected fields to use it as a Primary Key
        """
        hash_string = ''

        for key in fields_to_hash:
            hash_string += str(original_record.get(key, ''))

        hash_string_bytes = hash_string.encode('utf-8')
        hashed_string = hashlib.sha256(hash_string_bytes).hexdigest()

        # Add Primary Key hash in the record
        original_record['_sdc_record_hash'] = hashed_string
        return original_record

    @staticmethod
    def epoch_milliseconds_to_dt_str(timestamp: float) -> str:
        # Convert epoch milliseconds to datetime object in UTC format
        new_dttm = unix_milliseconds_to_datetime(timestamp)
        return new_dttm

    @staticmethod
    def dt_to_epoch_seconds(dt_object: datetime) -> float:
        return datetime.datetime.timestamp(dt_object)

    def sync_substream(self, parent_id, stream_schema, stream_metadata, parent_replication_key, state):
        """
            Sync sub-stream data based on parent id and update the state to parent's replication value.
            For Fireflies, we do not need to sync sub-stream. But we do need that for Aircall. 
        """
        raise NotImplementedError("In case that you need to sync substream, please refer to: https://github.com/singer-io/tap-intercom/blob/2d5e983bc437769ffaefb2dbd95f1ab596b47b2d/tap_intercom/streams.py#L91")

class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    to_write_intermediate_bookmark = False
    last_processed = None
    last_sync_started_at = None

    def set_last_processed(self, state):
        self.last_processed = None

    def get_last_sync_started_at(self, state):
        self.last_sync_started_at = None

    def set_last_sync_started_at(self, state):
        pass

    def skip_records(self, record):
        return False

    def write_bookmark(self, state, bookmark_value):
        return singer.write_bookmark(state,
                                     self.tap_stream_id,
                                     self.replication_key,
                                     bookmark_value)

    def write_intermediate_bookmark(self, state, last_processed, bookmark_value):
        if self.to_write_intermediate_bookmark:
            # Write bookmark and state after every page of records
            state = singer.write_bookmark(state,
                                          self.tap_stream_id,
                                          self.replication_key,
                                          singer.utils.strftime(bookmark_value))
            singer.write_state(state)

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument.So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """

        # Get current stream bookmark (Credit: it's modified from tap-intercom.)
        current_bookmark = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        current_bookmark_utc = singer.utils.strptime_to_utc(current_bookmark)
        sync_start_date = current_bookmark_utc
        self.set_last_processed(state)
        self.set_last_sync_started_at(state)

        LOGGER.info("Stream: {}, initial max_bookmark_value: {}".format(self.tap_stream_id, sync_start_date))
        max_datetime = sync_start_date
        # We are not using singer's record counter as the counter reset after 60 seconds
        record_counter = 0
        all_counter = 0

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(sync_start_date, stream_metadata=stream_metadata):
                all_counter += 1

                record_datetime = singer.utils.strptime_to_utc(
                    self.epoch_milliseconds_to_dt_str(
                        record[self.replication_key])
                )

                if record_datetime >= current_bookmark_utc:
                    record_counter += 1
                    transformed_record = transform(record,
                                                   stream_schema,
                                                   integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                   metadata=stream_metadata)
                    # Write record if a parent is selected
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

                if record_counter == MAX_PAGE_SIZE:
                    # For fireflies, we haven't enabled intermediate bookmark.
                    self.write_intermediate_bookmark(state, record.get("id"), max_datetime)
                    # Reset counter
                    record_counter = 0

                if all_counter % 1000 == 0:
                    LOGGER.info("Still Syncing: {}, total_records written so far: {}. total seen {}".format(self.tap_stream_id, record_counter, all_counter))

            bookmark_date = singer.utils.strftime(max_datetime)
            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, record_counter))

        LOGGER.info("Stream: {}, writing final bookmark".format(self.tap_stream_id))
        self.write_bookmark(state, bookmark_date)
        return state


# pylint: disable=abstract-method
class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'
    # Boolean flag to do sync with activate version for Company and Contact Attributes streams
    sync_with_version = False

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument. So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                counter.increment()

            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        return state


class Users(FullTableStream):
    """
    Retrieves users data using Fireflies GraphQL query.
    """
    tap_stream_id = "users"
    key_properties = ["user_id"]
    endpoint = "users"
    schema_key = "users"

    def get_records(self, bookmark_datetime=None) -> Iterator[list]:
        graphql_query = """
            query Users {
                users {
                    user_id
                    email
                    name
                    num_transcripts
                    recent_meeting
                    minutes_consumed
                    is_admin
                    integrations
                    user_groups {
                        name
                        handle
                    }
                }
            }
        """
        input_query = {
            "query": graphql_query
        }

        response = self.client.post(path=None, endpoint=self.endpoint, json=input_query)
        if not response.get(self.data_key):
            LOGGER.critical("response is empty for {} stream".format(self.tap_stream_id))
            raise FirefliesError
        
        yield from response.get(self.data_key).get(self.schema_key, [])

class Transcripts(IncrementalStream):
    """
    Retrieves transcript data using Fireflies GraphQL query, with manual pagination.
    """
    tap_stream_id = "transcripts"
    key_properties = ["id"]
    endpoint = "transcripts"
    schema_key = "transcripts"
    replication_key = "date"
    valid_replication_keys = ["date"]

    def get_records(self, bookmark_datetime: datetime.datetime = None, stream_metadata=None) -> Iterator[list]:
        paging = True
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))
        visited_id = set()

        graphql_query = """
            query Transcripts(
            $fromDate: DateTime
            $toDate: DateTime
            $limit: Int
            $skip: Int
            ) {
            transcripts(
                fromDate: $fromDate
                toDate: $toDate
                limit: $limit
                skip: $skip
            ) {
                id
                analytics {
                sentiments {
                    negative_pct
                    neutral_pct
                    positive_pct
                }
                categories {
                    questions
                    date_times
                    metrics
                    tasks
                }
                speakers {
                    speaker_id
                    name
                    duration
                    word_count
                    longest_monologue
                    monologues_count
                    filler_words
                    questions
                    duration_pct
                    words_per_minute
                }
                }
                sentences {
                index
                speaker_id
                text
                start_time
                end_time
                }
                title
                speakers {
                id
                name
                }
                organizer_email
                meeting_link
                meeting_info {
                fred_joined
                silent_meeting
                summary_status
                }
                participants
                date
                duration
                meeting_attendees {
                displayName
                email
                phoneNumber
                name
                location
                }
                summary {
                keywords
                action_items
                outline
                shorthand_bullet
                overview
                bullet_gist
                gist
                short_summary
                short_overview
                meeting_type
                }
            }
            } 
        """

        graphql_variables = {
            "fromDate": bookmark_datetime.isoformat(),
            "toDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "limit": FIREFLIES_MAX_NUM_OF_RECORDS,
            "skip": 0
        }

        while paging:
            LOGGER.info("In the process of paging. Current fromDate: {}, toDate: {}".format(graphql_variables["fromDate"], graphql_variables["toDate"]))
            graphql_input = {
                "query": graphql_query,
                "variables": graphql_variables
            }
            
            response = self.client.post(path=None, endpoint=self.endpoint, json=graphql_input)
            if not response.get(self.data_key):
                LOGGER.critical("response is empty for {} stream".format(self.tap_stream_id))
                raise FirefliesError
            
            records = response.get(self.data_key).get(self.schema_key, [])
            num_of_records = len(records)
            has_new_record = False
            next_toDate_in_unix_ts = None
            next_toDate_in_iso_string = None

            for record in records:
                transcript_id = record.get("id")
                summary_status = record.get("meeting_info", {}).get("summary_status")
                if next_toDate_in_unix_ts:
                    next_toDate_in_unix_ts = min(next_toDate_in_unix_ts, record.get("date"))
                else:
                    next_toDate_in_unix_ts = record.get("date")
                # Skip transcript that has been 'visited'
                if transcript_id in visited_id:
                    continue
                yield record
                has_new_record = True
                visited_id.add(transcript_id)
            
            if next_toDate_in_unix_ts:
                next_toDate_in_iso_dt = datetime.datetime.fromtimestamp(float(next_toDate_in_unix_ts) / 1000.0, datetime.timezone.utc)
                next_toDate_in_iso_string = next_toDate_in_iso_dt.isoformat()
                graphql_variables.update({
                    "toDate": next_toDate_in_iso_string
                    })
            
            # Need a way to stop paging
            if num_of_records < FIREFLIES_MAX_NUM_OF_RECORDS or not has_new_record or not next_toDate_in_iso_string:
                paging = False

STREAMS = {
    "users": Users,
    "transcripts": Transcripts,
}