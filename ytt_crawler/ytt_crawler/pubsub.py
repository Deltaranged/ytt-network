"""
Module for interfacing with the pub/sub architecture. This could be as
simple as a MQTT queue like Mosquitto, or a full-fledged Kafka cluster.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable, AsyncIterator
import json
import asyncio

import aiomqtt
from pydantic import BaseModel


class MissingDeserializerException(Exception):
    def __init__(self, topic):            
        super().__init__(
            f"No deserializer defined for topic '{topic}'"
        )


class PubsubQueue(ABC):
    def __init__(self, host: str, port: int):
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
    def __init__(self,
                 host: str,
                 port: int,
                 topic_deserializer: Dict[str, Callable]
                 ):
        super().__init__(host, port)
        self._topic_deserializer = topic_deserializer

    async def publish(self, topic: str, payload: BaseModel):
        payload_ser = payload.model_dump_json()
        async with aiomqtt.Client(self._host, self._port) as client:
            await client.publish(topic, payload=payload_ser)

    async def subscribe(self, topic: str) -> AsyncIterator[BaseModel]:
        if topic not in self._topic_deserializer:
            raise MissingDeserializerException(topic)

        async with aiomqtt.Client(self._host, self._port) as client:
            await client.subscribe(topic)
            async for message in client.messages:
                yield self._topic_deserializer[topic](message.payload)
