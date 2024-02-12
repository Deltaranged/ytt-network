"""
Module for interfacing with the pub/sub architecture. This could be as
simple as a MQTT queue like Mosquitto, or a full-fledged Kafka cluster.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List, Tuple, Dict, Any
import json
import asyncio

import aiomqtt


class PubsubQueue(ABC):
    def __init__(self, host: str, port: int = 1883):
        self._host = host
        self._port = port
        super().__init__()

    @abstractmethod
    async def publish(self, topic: str, payload: Dict):
        pass

    @abstractmethod
    async def subscribe(self, topic: str):
        pass


class MosquittoQueue(PubsubQueue):
    async def publish(self, topic: str, payload: Dict):
        payload = json.dumps(payload)
        async with aiomqtt.Client(self._host, self._port) as client:
            await client.publish(topic, payload=payload)

    async def subscribe(self, topic: str):
        async with aiomqtt.Client(self._host, self._port) as client:
            await client.subscribe(topic)
            async for message in client.messages:
                yield json.loads(message.payload)
