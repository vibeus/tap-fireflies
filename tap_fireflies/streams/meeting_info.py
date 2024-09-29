import singer
import time
from datetime import datetime
from singer import metadata
from tap_fireflies.custom_utils import GraphQLHelper

LOGGER = singer.get_logger()
FIREFLIES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SCHEMA_NAMES = ['transcripts']
CUTOFF_SCHEMA_NAME = 'transcripts'
CUTOFF_FIELD_NAME = 'date'

class MeetingInfo:
    def __init__(self):
        self._start_date = ""
        self._state = {}
    
    @property
    def name(self):
        return "meeting_info"

    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_key(self):
        return "date"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def state(self):
        return self._state

    def get_metadata(self, schema):
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=[self.replication_key],
            replication_method=self.replication_method,
        )
        return mdata 
    
    def get_tap_data(self, config, state):
        access_token = config["access_token"]
        endpoint_url = config["endpoint_url"]
        
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime(FIREFLIES_DATETIME_FORMAT)
        # In milliseconds
        self._start_date = 1000 * round(datetime.timestamp(datetime.strptime(config.get("start_date", today), FIREFLIES_DATETIME_FORMAT)))
        self._state = state.copy()

        state_date = self._state.get('meeting_info', self._start_date)
        cutoff_time = min(state_date, self._start_date)
        max_rep_key = cutoff_time

        fireflies_api_helper = GraphQLHelper(endpoint_url=endpoint_url, token=access_token)
        get_meeting_info_query = """
                                 query getMeetingInfo($limit: Int, $skip: Int) {
                                    transcripts(limit: $limit skip: $skip) {
                                        id
                                        title
                                        date
                                        organizer_email
                                        meeting_info {
                                            summary_status
                                        }
                                        summary {
                                            keywords
                                            overview
                                            shorthand_bullet
                                        }
                                    }
                                }
                                """
        meeting_info_data, num_of_records = fireflies_api_helper.execute_pagination_with_cutoff(query=get_meeting_info_query,
                                                                                                schema_names=SCHEMA_NAMES,
                                                                                                cutoff_schema=CUTOFF_SCHEMA_NAME,
                                                                                                cutoff_field=CUTOFF_FIELD_NAME,
                                                                                                cutoff_value=cutoff_time)
        LOGGER.info("We collect {} records from Fireflies".format(num_of_records))
        # In our use case, we only include one schema in one GraphQL query. 
        meeting_info_records = meeting_info_data[CUTOFF_SCHEMA_NAME]
        for meeting_info_record in meeting_info_records:
            # Need to exclude some meetings.
            # We only want the meeting with customers
            if "| Vibe" in meeting_info_record['title'] or "<>" in meeting_info_record['title']:
                rep_key = meeting_info_record.get(self.replication_key)
                if rep_key and rep_key > max_rep_key:
                    max_rep_key = rep_key
                yield self.data_processing(meeting_info_record)

        self._state['meeting_info'] = max_rep_key
    
    def data_processing(self, meeting_info_record):
        meeting_info_record["meeting_timestamp"] = meeting_info_record["date"]
        meeting_info_record.pop("date", None)

        meeting_info_record["summary_status"] = meeting_info_record["meeting_info"]["summary_status"]
        meeting_info_record.pop("meeting_info", None)

        meeting_info_record['overview'] = meeting_info_record['summary']['overview']
        meeting_info_record['shorthand_bullet'] = meeting_info_record['summary']['shorthand_bullet']
        meeting_info_record['keywords'] = ','.join(meeting_info_record['summary']['keywords'])
        meeting_info_record.pop('summary', None)

        if "| Vibe" in meeting_info_record["title"]:
            meeting_info_record['meeting_type'] = 'Demo meeting'
        else:
            meeting_info_record['meeting_type'] = 'Phone call'
        return meeting_info_record

