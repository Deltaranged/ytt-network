# Youtaite cover crawler

This module contains utilities for crawling Youtube covers scraped by the `ytt_scraper` module. The algorithm, at a high level, is a breadth-first search that starts from a given channel, video, or playlist, then proceeds to traverse the network given links in the video description, which are extracted by the scraper.

## Usage

Ideally, this will run with multiple workers in parallel, reading off a pub-sub architecture. As of the moment, we plan on using the following architecture:

* Python Ray for parallelization
* Mosquitto for MQTT pub-sub prototyping
* Apache Kafka as the pub-sub broker for production
* ArangoDB or PostgreSQL for the data store, depending on the nature of the graph