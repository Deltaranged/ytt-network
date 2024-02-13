"""
Data type definitions and schemas for interfacing within and outside the
module. Most will be written in Pydantic with appropriate ser/de.
"""

from datetime import datetime

from pydantic import BaseModel


class ChannelDetails(BaseModel):
    channel_id: str
    handle: str
    title: str
    description: str
    last_publish_time: datetime
    video_count: int

class VideoDetails(BaseModel):
    video_id: str
    channel_id: str
    publish_time: datetime
    title: str
    description: str
    cleaned_text: str | None