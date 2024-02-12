"""
Module for storing the BFS algorithm. Specifically, this module will
handle details like timeout management, rate limiting, etc.

This module will be the one to call the `ytt_scraper` module, and
will interface with the pub-sub tool as a queue for the BFS.
"""

import asyncio

from ytt_crawler.pubsub import MosquittoQueue

async def passthru(payload):
    await asyncio.sleep(1)
    queue = MosquittoQueue('localhost')

    await queue.publish('source/channels', payload)

async def receive():
    queue = MosquittoQueue('localhost')

    output = queue.subscribe('source/#')
    async for o in output:
        print(f'output received: {o}')

async def __async__main():
    await asyncio.gather(
        receive(),
        passthru({'vocalist': 'asdfasdf'}),
        passthru({'vocalist': 'weretwetwe'})
    )

def main():
    asyncio.run(__async__main())

if __name__ == "__main__":
    main()