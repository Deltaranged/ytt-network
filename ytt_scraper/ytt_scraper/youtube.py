"""
Module which contains Youtube API calls. Outputs are responses which are still
in the documented format.
"""

from typing import List, Dict, Any
from requests.exceptions import ConnectionError

import googleapiclient.discovery
import googleapiclient.errors

from ytt_scraper.config import get_youtube_credentials

API_SERVICE_NAME, API_VERSION, API_KEY = get_youtube_credentials()


def query_playlist_videos(playlist_id: str, max_results: int = 30) -> Dict[str, Any]:
    """
    Give the playlist ID, return the API response which contains the response
    """
    youtube = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=max_results
    )
    response = request.execute()

    if ('items' not in response) or (not response['items']):
        raise ConnectionError

    return response


def query_videos(video_ids: List[str]) -> Dict[str, Any]:
    """
    Give the video ID, return the API response which contains more video
    information
    """
    youtube = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    request = youtube.videos().list(
        part="snippet,contentDetails",
        id=",".join(video_ids)
    )
    response = request.execute()

    if ('items' not in response) or (not response['items']):
        raise ConnectionError

    return response


def query_channel_details(channel_handle: str) -> Dict[str, Any]:
    """
    Give the channel handle, return the API response which contains more
    information, particularly the channel ID.
    """
    youtube = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    request = youtube.channels().list(
        part="snippet,id,statistics",
        forHandle=channel_handle
    )
    response = request.execute()

    if ('items' not in response) or (not response['items']):
        raise ConnectionError

    return response


def query_channel_videos(channel_id: str, max_results: int = 30):
    """
    Give the channel ID, return the API response which contains a list of
    uploaded videos and livestreams
    """
    youtube = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=max_results,
        order="date"
    )
    response = request.execute()

    if ('items' not in response) or (not response['items']):
        raise ConnectionError

    return response