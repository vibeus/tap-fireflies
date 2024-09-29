from .meeting_info import MeetingInfo
from .meeting_attendees import MeetingAttendees

def create_stream(stream_id):
    if stream_id == "meeting_info":
        return MeetingInfo()
    elif stream_id == "meeting_attendees":
        return MeetingAttendees()
    
    assert False, f"Unsupported stream: {stream_id}"