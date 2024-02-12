"""
Main module for NER models. An abstraction is written to allow easy extension
between traditional parser models and possibly transformer-based models.

Model paths will be stored as configuration variables.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List, Tuple, Dict, Any

import spacy

from ytt_scraper.config import get_model_path


class NERModel(ABC):
    def __init__(self, classes: Iterable):
        self._classes = set(classes)
        super().__init__()

    @abstractmethod
    def extract_entities(self, text: str) -> List[Tuple]:
        """
        Given cleaned text input, extracts entities from the text. The output is
        raw in the sense that the actual entities of interest are abstract. More
        specific output is returned in a different function.
        """
        pass

    @abstractmethod
    def get_entities(self, entity_list: List[Tuple], entity: str) -> Dict[str, Any]:
        """
        Given an entity of interest, returns a dictionary of entities, mapping
        the entity to other related entities.
        """
        pass


class TransitionBasedParserModel(NERModel):
    def __init__(self, classes: Iterable, model_path: str = None):
        super().__init__(classes)

        if model_path is None:
            model_path = get_model_path()
        self._model_path = model_path
        self._model = spacy.load(model_path)

    def extract_entities(self, text: str) -> List[Tuple]:
        preds = self._model(text)
        return [(e.label_, e.text)
                for e in preds.ents
                if e.label_ in self._classes]

    def get_entities(self, entity_list: List[Tuple], entity: str) -> Dict[str, Any]:
        output = {}
        for (_label, _text) in entity_list:
            if _label == entity:
                output[_text[1:]] = _text
        return output