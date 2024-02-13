"""
Module for storing the BFS algorithm. Specifically, this module will
handle details like timeout management, rate limiting, etc.

This module will be the one to call the `ytt_scraper` module, and
will interface with the pub-sub tool as a queue for the BFS.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List, Tuple, Dict, Any
import json
import asyncio
from dotenv import load_dotenv
load_dotenv()

from ytt_crawler.pubsub import MosquittoQueue
from ytt_scraper import handler as ytt
from ytt_scraper import ner
from ytt_scraper.schema import ChannelDetails, VideoDetails


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
        self._ner_model = ner.model.TransitionBasedParserModel(
            classes=["ORIGINAL_LINK", "ORIGINAL_TITLE", "VOCALIST_LINK", "VOCALIST_NAME", "VOCALIST_REF"]
        )

    def run(self):
        asyncio.run(self.__async__run())
        
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
        print(f"-> Enqueueing {channel_handle}...")
        payload = await _async_get_channel_details(channel_handle)
        if not payload:
            print(f"{channel_handle} yielded no results, not enqueueing")
            return
        await self._queue.publish('source/channels', payload)

    async def _async_seed_queue(self):
        await asyncio.sleep(1)
        await self._async_enqueue_channel(self._start_channel)

    # Processing

    def _extract_vocalists_from_video(self, video: VideoDetails):
        entities = self._ner_model.extract_entities(video.cleaned_text)
        vocalists = self._ner_model.get_entities(entities, 'VOCALIST_REF')
        return vocalists

    # Subscribing / Crawling

    async def _async_crawl_channel_for_videos(self):
        channels = self._queue.subscribe('source/channels')
        async for channel in channels:
            print(f"Received channel {channel.handle} from queue, getting channel videos...")
            await asyncio.sleep(21)
            videos = await _async_get_videos_from_channel_id(channel.channel_id)
            clean_videos = [ner.preprocess.clean_video(v) for v in videos]
            filtered_videos = ner.preprocess.filter_videos(clean_videos)
            print(f"Obtained {len(videos)}->{len(filtered_videos)} videos from {channel.handle}, enqueueing...")
            for future in asyncio.as_completed(map(
                self._async_enqueue_video,
                filtered_videos
            )):
                await future


    async def _async_crawl_video_for_vocalists(self):
        videos = self._queue.subscribe('source/videos')
        async for video in videos:
            print(f"Received video {video.title} from queue, extracting vocalists...")
            await asyncio.sleep(5)
            vocalists = self._extract_vocalists_from_video(video)
            print(f"{len(vocalists)} vocalist/s detected, enqueueing...")
            for channel_handle in vocalists.keys():
                await asyncio.sleep(5)
                await self._async_enqueue_channel(channel_handle)


def main():
    print("Running crawler...", flush=True)
    crawler = BFSCrawler('Soshi', 'localhost')
    crawler.run()

if __name__ == "__main__":
    main()