import singer
import time
from datetime import datetime
from singer import metadata

LOGGER = singer.get_logger()

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
        return "meeting_datetime"

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
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self._start_date = round(datetime.timestamp(datetime.strptime(config.get("start_date", today), "%Y-%m-%dT%H:%M:%S.%fZ")))
        self._state = state.copy()