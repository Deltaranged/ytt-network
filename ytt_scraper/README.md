# Youtaite cover scraper

This module contains functions for calling the Youtube API, filtering cover
videos of interest, and parsing the description to get more information on
collab partners, which will then be scraped further. The actual network
traversal will be done elsewhere. This module has 3 parts:

* API layer, which makes calls to Youtube
* Handler layer, which facilitate the movement of data and external API
* NER model, which processes the text to extract the entities of interest