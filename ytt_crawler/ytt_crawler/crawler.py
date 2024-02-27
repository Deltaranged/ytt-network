"""
Module for storing the BFS algorithm. Specifically, this module will
handle details like timeout management, rate limiting, etc.

This module will be the one to call the `ytt_scraper` module, and
will interface with the pub-sub tool as a queue for the BFS.
"""

import os
from abc import ABC, abstractmethod
from typing import Iterable, List, Tuple, Dict, Any
import json
import asyncio
import cachetools

from dotenv import load_dotenv
load_dotenv()

from ytt_crawler.pubsub import MosquittoQueue
from ytt_scraper import handler as ytt
from ytt_scraper import ner
from ytt_database.schema import ChannelDetails, VideoDetails
from ytt_database.handler import YttDatabase


TOPIC_DESERIALIZER = {
    'source/channels': ChannelDetails.model_validate_json,
    'source/videos': VideoDetails.model_validate_json,
}

async def _async_get_channel_details(channel_handle: str):
    return ytt.get_channel_details(channel_handle)

async def _async_get_video_from_video_id(video_id: str):
    return ytt.get_video_from_video_id(video_id)

async def _async_get_videos_from_channel_id(video_id: str):
    return ytt.get_videos_from_channel_id(video_id)

async def _async_get_videos_from_playlist_id(video_id: str):
    return ytt.get_videos_from_playlist_id(video_id)


class BFSCrawler():
    def __init__(self,
                 start_channel: str,
                 queue_host: str,
                 queue_port: int = 1883):
        self._start_channel = start_channel
        self._queue = MosquittoQueue(queue_host, queue_port, TOPIC_DESERIALIZER)
        self._ner_model = ner.model.RegexBasedParserModel(
            classes=["VOCALIST_REF"]
        )

        self._wait_times = {}
        self._wait_times['seed'] = int(os.environ.get('CRAWLER_SEED_WAIT_TIME', 1))
        self._wait_times['channel_id'] = int(os.environ.get('CRAWLER_CHANNEL_ID_WAIT_TIME', 21))
        self._wait_times['video_id'] = int(os.environ.get('CRAWLER_VIDEO_ID_WAIT_TIME', 5))

        # 128 entries, 10 minutes
        self._channel_ttl_cache = cachetools.TTLCache(maxsize=128, ttl=10 * 60)

        self.setup_db()

    def run(self):
        asyncio.run(self.__async__run())

    def setup_db(self):
        self._db = YttDatabase()
        
    # Async

    async def __async__run(self):
        await asyncio.gather(
            self._async_seed_queue(),
            self._async_crawl_channel_for_videos(),
            self._async_crawl_video_for_vocalists()
        )

    # Publishing

    async def _async_enqueue_video(self, video: VideoDetails):
        print(f"-> Enqueueing {video.title}...")
        await self._queue.publish('source/videos', video)

    async def _async_enqueue_channel(self, channel_handle: str):
        if ((channel_handle in self._channel_ttl_cache)
            or (self._db.get_channel_by_handle(channel_handle) is None)):
            print(f"{channel_handle} already enqueued, skipping")
        print(f"-> Enqueueing {channel_handle}...")
        payload = await _async_get_channel_details(channel_handle)
        if not payload:
            print(f"{channel_handle} yielded no results, not enqueueing")
            return
        await self._queue.publish('source/channels', payload)
        self._channel_ttl_cache[channel_handle] = 1

    async def _async_seed_queue(self):
        await asyncio.sleep(self._wait_times['seed'])
        await self._async_enqueue_channel(self._start_channel)

    # Processing

    def _extract_vocalists_from_video(self, video: VideoDetails):
        entities = self._ner_model.extract_entities(video.cleaned_text)
        print(entities)
        vocalists = self._ner_model.get_entities(entities, 'VOCALIST_REF')
        return vocalists

    # Subscribing / Crawling

    async def _async_crawl_channel_for_videos(self):
        channels = self._queue.subscribe('source/channels')
        async for channel in channels:
            print(f"Received channel {channel.handle} from queue, getting channel videos...")
            await self._async_insert_channel_to_db(channel)

            videos = await _async_get_videos_from_channel_id(channel.channel_id)

            clean_videos: List[VideoDetails] = \
                [ner.preprocess.clean_video(v) for v in videos]
            filtered_videos: List[VideoDetails] = \
                ner.preprocess.filter_videos(clean_videos)
            print(f"Obtained {len(videos)}->{len(filtered_videos)} videos from {channel.handle}, enqueueing...")

            for future in asyncio.as_completed(map(
                self._async_insert_video_to_db,
                filtered_videos
            )):
                await future

            for future in asyncio.as_completed(map(
                self._async_enqueue_video,
                filtered_videos
            )):
                await future

            await asyncio.sleep(self._wait_times['channel_id'])


    async def _async_crawl_video_for_vocalists(self):
        videos = self._queue.subscribe('source/videos')
        async for video in videos:
            print(f"Received video {video.title} from queue, extracting vocalists...")
            vocalists = self._extract_vocalists_from_video(video)
            print(f"{len(vocalists)} vocalist/s detected, enqueueing...")

            for future in asyncio.as_completed(map(
                self._async_enqueue_channel,
                vocalists.keys()
            )):
                await future

            vocalist_details = [
                await future
                for future in asyncio.as_completed(map(
                    _async_get_channel_details,
                    vocalists.keys()
                ))
            ]

            vocalist_edges = [
                (video.video_id, vd.channel_id)
                for vd in vocalist_details
                if vd
            ]
            for future in asyncio.as_completed(map(
                self._async_insert_vocalist_to_db,
                vocalist_edges
            )):
                await future

            await asyncio.sleep(self._wait_times['video_id'])

    # Inserting to DB (will be replaced perhaps)

    async def _async_insert_video_to_db(self, video: VideoDetails):
        self._db.insert_video(video)

    async def _async_insert_channel_to_db(self, channel: ChannelDetails):
        self._db.insert_channel(channel)

    async def _async_insert_vocalist_to_db(self, ids: Tuple):
        self._db.insert_vocalist_edge(*ids)


def main():
    print("Running crawler...", flush=True)
    crawler = BFSCrawler('Soshi', 'localhost')
    crawler.run()

if __name__ == "__main__":
    main()