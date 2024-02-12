"""
Submodule for cleaning up description data for the NER model.
"""

import re
from typing import List, Dict

from unidecode import unidecode

NONWORD_REGEX = re.compile(r"[^\w+:/\\.#\=\-\?\’'\<\>@\n\u3040-\u309F\u30A0-\u30FF\u4300-\u9faf]")
COVER_REGEX = re.compile(r"cover|tsutemita|utattemita|utaite", re.IGNORECASE)


def custom_tokenizer(text: str, return_list: bool = True):
    _text = re.sub('\\n', ' <NEWLINE> ', text)
    _text = re.sub('[ \t\r\f]+', ' ', _text)
    if return_list:
        return _text.split(' ')
    return _text


def clean_text(text: str):
    unidecoded_text = unidecode(text)
    subbed_text = re.sub(NONWORD_REGEX, ' ', unidecoded_text)
    return custom_tokenizer(subbed_text, return_list=False)


def clean_video(v: Dict):
    v['cleaned_title'] = clean_text(v['title'])
    v['cleaned_description'] = clean_text(v['description'])
    return v


def clean_videos(videos: List):
    output = []
    for v in videos:
        v['cleaned_title'] = clean_text(v['title'])
        v['cleaned_description'] = clean_text(v['description'])
        output.append(v)
    return output


def filter_videos(videos: List) -> List:
    """
    Filters out videos which do not have certain keywords in the title or
    description
    """
    return [
        v for v in videos
        if (
            (COVER_REGEX.search(v['cleaned_title']))
            or (COVER_REGEX.search(v['cleaned_description']))
        )
    ]