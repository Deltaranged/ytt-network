"""
Sub-module that will expose functions for interacting with the ArangoDB
"""

from arango import ArangoClient
from arango.exceptions import DocumentInsertError

from ytt_database.schema import (
    ChannelDetails,
    VideoDetails
)

class YttDatabase:
    def __init__(self, hosts='http://localhost:8529'):
        self._client = ArangoClient(hosts=hosts)
        self._db = self._client.db('ytt_db', username='root', password='password')
        self._graph = self._db.graph('ytt_network')

    # Access

    def get_channel(self, channel_id):
        coll = self._db.collection('channel')
        return coll.get({'_key': channel_id})

    def get_channel_by_handle(self, channel_handle):
        coll = self._db.collection('channel')
        res = coll.find({'handle': channel_handle})
        if len(res) < 1:
            return None
        return next(res)

    def get_video(self, video_id):
        coll = self._db.collection('video')
        return coll.get({'_key': video_id})


    # Insert

    def insert_channel(self, channel: ChannelDetails):
        coll = self._db.collection('channel')
        _channel = channel.model_dump()
        _channel['_key'] = _channel.pop('channel_id')

        try:
            coll.insert(_channel)
        except DocumentInsertError as e:
            # probably key conflict (i.e. document already exists)
            print(f'Insert error (ignoring): {repr(e)}')

    def insert_video(self, video: VideoDetails):
        coll = self._db.collection('video')
        _video = video.model_dump()
        video_id = _video.pop('video_id')
        channel_id = _video.pop('channel_id')
        _video['_key'] = video_id

        try:
            coll.insert(_video)
            self.insert_upload_edge(channel_id, video_id)
        except DocumentInsertError as e:
            # probably key conflict (i.e. document already exists)
            print(f'Insert error (ignoring): {repr(e)}')

    def insert_upload_edge(self, channel_id, video_id):
        coll = self._db.collection('upload')
        payload = {
            '_key': f'{channel_id}-{video_id}',
            '_from': f'channel/{channel_id}',
            '_to': f'video/{video_id}',
        }
        try:
            coll.insert(payload)
        except DocumentInsertError as e:
            # probably key conflict (i.e. document already exists)
            print(f'Insert error (ignoring): {repr(e)}')

    def insert_vocalist_edge(self, video_id, channel_id):
        coll = self._db.collection('vocalist')
        payload = {
            '_key': f'{video_id}-{channel_id}',
            '_from': f'video/{video_id}',
            '_to': f'channel/{channel_id}',
        }
        try:
            coll.insert(payload)
        except DocumentInsertError as e:
            # probably key conflict (i.e. document already exists)
            print(f'Insert error (ignoring): {repr(e)}')