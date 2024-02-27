"""
Data type definitions and schemas for interfacing within the module.
Most will be written in Pydantic with appropriate ser/de.
"""

from datetime import datetime

from pydantic import BaseModel, field_serializer


# Nodes

class ChannelNode(BaseModel):
    _key: str
    handle: str
    title: str
    description: str
    last_publish_time: datetime
    video_count: int

class VideoNode(BaseModel):
    _key: str
    publish_time: datetime
    title: str
    description: str
    cleaned_text: str | None


# Edges

class VocalistEdge(BaseModel):
    _key: str
    _from: str
    _to: str

class UploadEdge(BaseModel):
    _key: str
    _from: str
    _to: str


# Scraper objects

class ChannelDetails(BaseModel):
    channel_id: str
    handle: str
    title: str
    description: str
    last_publish_time: datetime
    video_count: int

    @field_serializer('last_publish_time')
    def serialize_last_publish_time(self, last_publish_time: datetime, _info):
        return last_publish_time.strftime("%Y-%m-%dT%H:%M:%S")

class VideoDetails(BaseModel):
    video_id: str
    channel_id: str
    publish_time: datetime
    title: str
    description: str
    cleaned_text: str | None

    @field_serializer('publish_time')
    def serialize_publish_time(self, publish_time: datetime, _info):
        return publish_time.strftime("%Y-%m-%dT%H:%M:%S")