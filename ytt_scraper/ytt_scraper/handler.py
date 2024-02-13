"""
Module which contains functions which interface with the Youtube and NER layers.
These are the methods exposed as the API for other code.
"""

from typing import List, Dict, Any
from requests.exceptions import ConnectionError

from ytt_scraper import youtube as yt
from ytt_scraper.schema import ChannelDetails, VideoDetails


# ===== Internal functions ===== #

def _get_playlist_video_ids(playlist_id: str) -> List[str]:
    try:
        details = yt.query_playlist_videos(playlist_id)
    except ConnectionError:
        print("Connection failed while trying to get playlist videos")
        return []

    items = details['items']
    return [item['snippet']['resourceId']['videoId']
            for item in items
            if item['snippet']['resourceId']['kind'] == 'youtube#video']


def _get_channel_video_ids(channel_id: str) -> List[str]:
    try:
        details = yt.query_channel_videos(channel_id)
    except ConnectionError:
        print("Connection failed while trying to get channel videos")
        return []

    items = details['items']
    return [item['id']['videoId']
            for item in items
            if item['id']['kind'] == 'youtube#video']


def _get_videos(video_ids: List[str]) -> List[VideoDetails]:
    try:
        videos = yt.query_videos(video_ids)
    except ConnectionError:
        print("Connection failed while trying to get videos")
        return []

    items = videos['items']
    return [
        VideoDetails(
            video_id=item['id'],
            channel_id=item['snippet']['channelId'],
            publish_time=item['snippet']['publishedAt'],
            title=item['snippet']['title'],
            description=item['snippet']['description'],
            cleaned_text=None
        )
        for item in items
    ]


# ===== External functions ===== #

def get_channel_details(channel_handle: str) -> ChannelDetails:
    try:
        details = yt.query_channel_details(channel_handle)
    except ConnectionError:
        print("Connection failed while trying to get channel information")
        return {}

    item = details['items'][0]
    return ChannelDetails(
        channel_id=item['id'],
        handle=channel_handle,
        title=item['snippet']['title'],
        description=item['snippet']['description'],
        last_publish_time=item['snippet']['publishedAt'],
        video_count=int(item['statistics']['videoCount'])
    )


def get_videos_from_channel_id(channel_id: str) -> List[VideoDetails]:
    video_ids = _get_channel_video_ids(channel_id)
    videos = _get_videos(video_ids)
    return videos


def get_videos_from_playlist_id(playlist_id: str) -> List[VideoDetails]:
    video_ids = _get_playlist_video_ids(playlist_id)
    videos = _get_videos(video_ids)
    return videos


def get_video_from_video_id(video_id: str) -> List[VideoDetails]:
    videos = _get_videos([video_id])
    return videos