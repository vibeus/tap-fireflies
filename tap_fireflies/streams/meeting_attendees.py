import singer
import time
from datetime import datetime
from singer import metadata
from tap_fireflies.custom_utils import GraphQLHelper

LOGGER = singer.get_logger()
FIREFLIES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SCHEMA_NAMES = ['transcripts']
CUTOFF_SCHEMA_NAME = 'transcripts'
CUTOFF_FIELD_NAME = 'meeting_timestamp'

class MeetingAttendees:
    def __init__(self):
        self._start_date = ""
        self._state = {}
    
    @property
    def name(self):
        return "meeting_attendees"

    @property
    def key_properties(self):
        return ["id", "email"]

    @property
    def replication_key(self):
        return "meeting_timestamp"

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

        state_date = self._state.get('meeting_timestamp', self._start_date)
        cutoff_time = min(state_date, self._start_date)
        max_rep_key = cutoff_time

        fireflies_api_helper = GraphQLHelper(endpoint_url=endpoint_url, token=access_token)
        get_meeting_attendees_query = """
                                 query getMeetingAttendees($limit: Int, $skip: Int) {
                                    transcripts(limit: $limit skip: $skip) {
                                        id
                                        title
                                        meeting_timestamp: date
                                        meeting_attendees {
                                            email
                                            phoneNumber
                                            location
                                            name
                                        }    
                                    }
                                }
                                """
        meeting_attendees_data, num_of_records = fireflies_api_helper.execute_pagination_with_cutoff(query=get_meeting_attendees_query,
                                                                                                schema_names=SCHEMA_NAMES,
                                                                                                cutoff_schema=CUTOFF_SCHEMA_NAME,
                                                                                                cutoff_field=CUTOFF_FIELD_NAME,
                                                                                                cutoff_value=cutoff_time)
        LOGGER.info("We collect {} records from Fireflies".format(num_of_records))
        # In our use case, we only include one schema in one GraphQL query. 
        meeting_attendees_records = meeting_attendees_data[CUTOFF_SCHEMA_NAME]
        for meeting_attendee_record in meeting_attendees_records:
            # Need to exclude some meetings.
            # We only want the meeting with customers
            if "| Vibe" in meeting_attendee_record['title'] or "<>" in meeting_attendee_record['title']:
                rep_key = meeting_attendee_record.get(self.replication_key)
                if rep_key and rep_key > max_rep_key:
                    max_rep_key = rep_key
                attendee_info_list = self.data_processing(meeting_attendee_record)
                for attendee_info in attendee_info_list:
                    yield attendee_info

        self._state['meeting_timestamp'] = max_rep_key        

    def data_processing(self, meeting_attendee_record):
        list_of_attendee_info = []
        meeting_id = meeting_attendee_record['id']
        meeting_timestamp = meeting_attendee_record['meeting_timestamp']
        for attendee in meeting_attendee_record['meeting_attendees']:
            email = attendee['email']
            phone_number = attendee['phoneNumber']
            location = attendee['location']
            name = attendee['name']
            attendee_info = {
                'id': meeting_id,
                'meeting_timestamp': meeting_timestamp,
                'email': email,
                'phone_number': phone_number,
                'location': location,
                'name': name
            }
            list_of_attendee_info.append(attendee_info)
        return list_of_attendee_info

        