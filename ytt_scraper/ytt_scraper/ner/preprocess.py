"""
Submodule for cleaning up description data for the NER model.
"""

import re
from typing import List, Dict

from unidecode import unidecode

from ytt_scraper.schema import VideoDetails

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


def clean_video(video: VideoDetails) -> VideoDetails:
    text = video.title + ' ' + video.description
    video.cleaned_text = clean_text(text)
    return video


def filter_videos(videos: List[VideoDetails]) -> List[VideoDetails]:
    """
    Filters out videos which do not have certain keywords in the title or
    description
    """
    return [
        v for v in videos
        if COVER_REGEX.search(v.cleaned_text)
    ]