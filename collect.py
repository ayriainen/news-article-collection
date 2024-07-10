import asyncio
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
import aiohttp
import feedparser

import database

def clean_url( url ):
    parsed = urlparse(url)
    qd = parse_qs(parsed.query, keep_blank_values=True)
    filtered = dict( (k, v) for k, v in qd.items() if not k.startswith('utm_'))
    newurl = urlunparse([
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        urlencode(filtered, doseq=True), # query string
        parsed.fragment
    ])
    return newurl

## some services contain a redirection
async def fetch( session, url ):
    try:
        async with session.get ( url, allow_redirects = False, timeout=aiohttp.ClientTimeout(total=10) ) as response:
            if 300 <= response.status < 400: ## detect redirections
                return response.headers['Location']
            return url
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

async def process_feed( session, feed_url ):
    try:
        feed = feedparser.parse( feed_url )
    except Exception as e:
        print(f"Error parsing feed URL {feed_url}: {e}")
        return []

    new_urls = []

    # list async fetch tasks for the items of the feed and handle them
    tasks = [fetch(session, item['link']) for item in feed['items']]
    try:
        for task in asyncio.as_completed(tasks, timeout=60):
            try:
                link = await asyncio.wait_for(task, timeout=12)
                if link:
                    cleaned_link = clean_url( link )
                    with database.session_scope() as db_session:
                        try:
                            ## check if we already have this URL
                            has_url = db_session.query( database.urls ).filter( database.urls.c.url == cleaned_link ).first()
                            if not has_url: ## have not collected item yet
                                new_urls.append(
                                    {
                                        'feed': feed_url,
                                        'url': cleaned_link
                                    }
                                )
                                print( cleaned_link )
                        except Exception as e:
                            print(f"Error appending link {link}: {e}")
            except asyncio.TimeoutError:
                print(f"Timeout fetching URL {task.get_coro().cr_frame.f_locals['url']}")
    except asyncio.TimeoutError:
        print(f"Timeout when collecting feed {feed_url}")
    except Exception as e:
        print(f"Error collecting feed {feed_url}: {e}")

    return new_urls

async def main():
    # parse and verify feeds.txt feed urls
    with open("data/feeds.txt") as f:
        feed_urls = [url.strip() for url in f if url.strip()]
    feed_urls = [url for url in feed_urls if urlparse(url).scheme in ['http', 'https']]
    if not feed_urls:
        print("No valid feed URLs found.")
        return

    # async session for processing
    async with aiohttp.ClientSession() as session:
        tasks = [process_feed(session, feed_url) for feed_url in feed_urls]
        results = await asyncio.gather(*tasks)

    # list of lists into single list
    new_entries = [entry for sublist in results for entry in sublist]

    if new_entries:
        with database.session_scope() as db_session:
            try:
                db_session.execute(database.urls.insert(), new_entries)
                db_session.commit()
                print("New URLs inserted into the database.")
            except Exception as e:
                print(f"Error inserting new URLs: {e}")

if __name__ == '__main__':
    asyncio.run(main())