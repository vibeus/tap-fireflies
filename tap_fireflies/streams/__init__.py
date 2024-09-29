from .meeting_info import MeetingInfo

def create_stream(stream_id):
    if stream_id == "meeting_info":
        return MeetingInfo()
    
    assert False, f"Unsupported stream: {stream_id}"