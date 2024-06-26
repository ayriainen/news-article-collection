# News article collection infrastructure

1. Periodically check if there are any new items in news RSS feeds.
1. If there are, store new article URL.
1. Download articles, store automated extraction of news stories as well as the full HTML.

## Inputting feeds

Place the RSS feeds in "feeds.txt" inside a "data" folder, each on their own line.

## Cronjob example

```
*/5 * * * * cd ~/news-article-collection/; python3 collect.py
*/30 * * * * cd ~/news-article-collection/; python3 process.py
```
